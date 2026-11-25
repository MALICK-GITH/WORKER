"""Ingestion privée des résultats réels collectés par Docker.

Aucun réseau ni initialisation de base à l'import. Les scores bruts sont
revalidés ici, contre les jambes enregistrées, avant le règlement VERITAS.
Les crons et les fournisseurs historiques restent inchangés.
"""
import datetime as dt
from collections import defaultdict

from flask import jsonify, request
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

from models import (db, TrackedPrediction, TrackingMatchResult, VeritasJobLock,
                    VeritasReviewQueue)
from tracking_service import (_refresh_coupons, notify_settled_coupons,
                              record_external_results)
from veritas import starz_results, store, validator
from veritas.resolver import normalise_name

MAX_BATCH = 100
MAX_BODY = 512 * 1024


def real_coupon_query():
    return TrackedPrediction.query.filter(
        TrackedPrediction.source == "coupon",
        TrackedPrediction.coupon_id.isnot(None),
        or_(TrackedPrediction.engine == "CALIBRE",
            TrackedPrediction.engine.like("CALIBRE %"),
            TrackedPrediction.engine == "calibre"))


def _ref(pick):
    return {"match_id": pick.match_id, "home": pick.home, "away": pick.away,
            "league": pick.league,
            "kickoff_ts": int(pick.kickoff.replace(tzinfo=dt.timezone.utc).timestamp())}


def ingest_rows(rows, observed_at=None):
    """Un lot, un verrou commun, une transaction, puis les push existants.

    Pas de promotion de snapshot ni de règle de disparition : une API vide ne
    change aucune sélection. Un score final contradictoire passe en revue,
    jamais dans la règle « le total le plus élevé » (prolongations possibles).
    """
    now = observed_at or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    report = {"imported": 0, "settled": 0, "rejected": 0, "review": 0,
              "busy": False}
    if not rows:
        return report
    ids = {str(row.get("id") or "") for row in rows}
    try:
        owner = store.acquire_lock(db.session, store.LOCK_NAME, ttl_seconds=240)
    except IntegrityError:
        db.session.rollback()
        owner = None
    if not owner:
        report["busy"] = True
        return report
    pushes = []
    committed = False
    try:
        # Maintient la ligne du bail verrouillée pendant la transaction courte.
        # Un autre cron ne peut pas reprendre le bail au milieu du règlement.
        lease = (VeritasJobLock.query.filter_by(name=store.LOCK_NAME, owner=owner)
                 .with_for_update().first())
        if lease is None or lease.expires_at <= now:
            raise RuntimeError("bail de règlement expiré")
        picks = (real_coupon_query().filter(
            TrackedPrediction.status.in_(store.PENDING_STATES),
            TrackedPrediction.match_id.in_(ids),
            TrackedPrediction.kickoff.isnot(None))
            .with_for_update().all())
        by_id = defaultdict(list)
        for pick in picks:
            by_id[pick.match_id].append(pick)
        # Aucune preuve partagée avec une sélection d'un autre monde/moteur.
        real_ids = real_coupon_query().with_entities(TrackedPrediction.id)
        collisions = {row[0] for row in db.session.query(TrackedPrediction.match_id)
                      .filter(TrackedPrediction.match_id.in_(ids),
                              ~TrackedPrediction.id.in_(real_ids)).all()}
        touched = set()
        grouped = defaultdict(list)
        for raw in rows:
            grouped[str(raw.get("id") or "")].append(raw)
        for match_id, raw_group in grouped.items():
            targets = by_id.get(match_id, [])
            if not targets or match_id in collisions:
                report["rejected"] += len(raw_group)
                continue
            parsed = [[(pick, starz_results.observation(raw, _ref(pick), observed_at=now))
                       for pick in targets] for raw in raw_group]
            # Toutes les jambes et toutes les lignes doivent être cohérentes.
            if any(item is None for group in parsed for _, item in group):
                report["rejected"] += len(raw_group)
                continue
            observations = parsed[0]
            item = observations[0][1]
            lot_conflict = len({(group[0][1].home_score, group[0][1].away_score)
                                for group in parsed}) > 1
            verdict = validator.validate_observation(item)
            if lot_conflict:
                verdict = validator.Verdict(validator.REVIEW_REQUIRED,
                    ("scores à 90 minutes contradictoires dans le même lot",))
            previous = db.session.get(TrackingMatchResult, match_id)
            identity_conflict = bool(previous and (
                (normalise_name(previous.home), normalise_name(previous.away)) !=
                (normalise_name(item.home), normalise_name(item.away)) or
                (previous.league and normalise_name(previous.league) !=
                 normalise_name(item.league)) or
                (previous.kickoff is not None and
                 abs((previous.kickoff - item.kickoff).total_seconds()) > 900)))
            score_conflict = bool(previous and previous.finish_marker and
                previous.home_score is not None and previous.away_score is not None and
                (previous.home_score, previous.away_score) !=
                (item.home_score, item.away_score))
            if identity_conflict:
                verdict = validator.Verdict(validator.REVIEW_REQUIRED,
                    ("identité du résultat enregistré incompatible (équipes, ligue ou horaire)",))
            elif score_conflict:
                verdict = validator.Verdict(validator.REVIEW_REQUIRED,
                    ("résultat enregistré incompatible avec le score à 90 minutes",))
            # Une réponse ultérieure isolée ne résout pas une contradiction.
            # Le blocage vaut aussi pour une nouvelle jambe du même match ;
            # seule la résolution explicite de la revue lève ce doute.
            open_review = (VeritasReviewQueue.query
                           .filter_by(match_id=match_id, status="open")
                           .order_by(VeritasReviewQueue.id).first())
            if verdict.settleable and open_review is not None:
                verdict = validator.Verdict(validator.REVIEW_REQUIRED,
                                            (open_review.reason,))
            if not verdict.settleable:
                for group in parsed:
                    proof = group[0][1]
                    store.record_provenance(db.session, proof,
                        evidence=proof.finish_evidence, source=starz_results.SOURCE_NAME)
                for pick, observation in observations:
                    store.settle_pick_row(db.session, pick, observation, verdict,
                                          payload_hash=observation.raw_hash)
                report["review"] += 1
                # Ne recalcule pas le coupon sur une preuve rejetée.
                continue
            report["imported"] += record_external_results(
                [item], source=starz_results.SOURCE_NAME, label=starz_results.LABEL)
            for pick, observation in observations:
                outcome = store.settle_pick_row(db.session, pick, observation, verdict,
                                                payload_hash=observation.raw_hash)
                if outcome.get("created"):
                    report["settled"] += 1
                    touched.add(pick.coupon_id)
            db.session.flush()
        _refresh_coupons(pushes, touched, coupon_ids=touched)
        if lease.expires_at <= dt.datetime.now(dt.timezone.utc).replace(tzinfo=None):
            raise RuntimeError("bail de règlement expiré")
        db.session.commit()
        committed = True
    except Exception:
        # release_lock fait un commit : rollback impératif avant son appel.
        db.session.rollback()
        raise
    finally:
        try:
            if not store.release_lock(db.session, store.LOCK_NAME, owner):
                report["lock_cleanup_failed"] = True
        except Exception:
            db.session.rollback()
            if committed:
                # Le règlement est déjà validé : ne pas annoncer un rollback
                # fictif ni empêcher l'envoi des notifications préparées.
                report["lock_cleanup_failed"] = True
            # Avant commit, l'exception d'origine continue de remonter.
    # La transaction est terminée avant tout appel Firebase ; un bail dont le
    # nettoyage a échoué expire naturellement, et son échec est signalé.
    try:
        notify_settled_coupons(pushes)
    except Exception:
        # Même un échec d'import du service push intervient APRÈS le commit.
        # Les notifications en boîte et les règlements restent enregistrés.
        db.session.rollback()
        report["notification_dispatch_failed"] = True
    return report


def register_coupon_results_routes(app, authorize):
    """La vérification du secret est injectée par le serveur, pas dupliquée."""
    @app.get("/cron/coupon-results/refs")
    def coupon_results_refs():
        if not authorize():
            return jsonify({"error": "Unauthorized"}), 401
        try:
            after = int(request.args.get("after", "0"))
            if after < 0:
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({"error": "Curseur invalide"}), 400
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        query = real_coupon_query().filter(
            TrackedPrediction.status.in_(store.PENDING_STATES),
            TrackedPrediction.kickoff <= now,
            TrackedPrediction.kickoff.isnot(None))
        picks = (query.filter(TrackedPrediction.id > after)
                 .order_by(TrackedPrediction.id).limit(MAX_BATCH).all())
        # Reboucle sur les anciens matchs non résolus sans les abandonner.
        if not picks and after:
            picks = query.order_by(TrackedPrediction.id).limit(MAX_BATCH).all()
        unique = {pick.match_id: _ref(pick) for pick in picks}
        return jsonify({"items": list(unique.values()),
                        "next_cursor": picks[-1].id if picks else 0})

    @app.post("/cron/coupon-results/ingest")
    def coupon_results_ingest():
        if not authorize():
            return jsonify({"error": "Unauthorized"}), 401
        if request.content_length is None or request.content_length > MAX_BODY:
            return jsonify({"error": "Lot trop volumineux ou taille absente"}), 413
        data = request.get_json(silent=True)
        rows = data.get("rows") if isinstance(data, dict) else None
        if not isinstance(rows, list) or len(rows) > MAX_BATCH or any(
                not isinstance(row, dict) for row in rows):
            return jsonify({"error": "Liste rows invalide (100 résultats maximum)"}), 400
        try:
            report = ingest_rows(rows)
        except Exception as exc:
            # Aucun secret ni détail de connexion dans la réponse publique.
            app.logger.error("Échec de l'ingestion des résultats réels (%s) ; transaction annulée",
                             type(exc).__name__)
            return jsonify({"error": "Échec du règlement ; transaction annulée"}), 500
        return jsonify(report), (202 if report["busy"] else 200)
