"""Tests for signal fusion, thresholds and the safety rules.

Section 7.2 of the specification is the contract under test here: every rule that
protects a photograph from an automatic reject gets its own case.
"""

from filecluster.curation import reasons
from filecluster.curation.configuration import CurationSettings
from filecluster.curation.scoring import (
    Signals,
    Verdict,
    confidence_for,
    fuse,
    has_protected_subject,
    keep_score,
    needs_vlm_escalation,
    resolve_signals,
    semantic_reason_for,
)
from filecluster.curation.types import CurationDecision

SEMANTIC = (reasons.SEMANTIC_UTILITY,)


class TestResolveSignals:
    def test_semantic_values_win_over_heuristics(self):
        signals = resolve_signals(
            {
                "personal_probability": 0.9,
                "utility_probability": 0.1,
                "rules.personal_prior": 0.2,
                "features.document_evidence": 0.8,
            }
        )

        assert signals.personal_probability == 0.9
        assert signals.utility_probability == 0.1
        assert signals.personal_source == "semantic"

    def test_heuristics_combine_metadata_and_pixels(self):
        signals = resolve_signals(
            {"rules.personal_prior": 0.8, "features.photographic_evidence": 0.3}
        )

        assert 0.3 < signals.personal_probability < 0.8
        assert signals.personal_source == "heuristic"

    def test_utility_takes_the_strongest_evidence(self):
        signals = resolve_signals(
            {
                "rules.utility_evidence": 0.2,
                "features.document_evidence": 0.4,
                "ocr.text_density_evidence": 0.9,
            }
        )

        assert signals.utility_probability == 0.9
        assert signals.utility_source == "ocr"

    def test_nothing_measured_stays_none(self):
        signals = resolve_signals({})

        assert signals.personal_probability is None
        assert signals.utility_probability is None
        assert signals.personal_source == "none"


class TestKeepScore:
    def test_missing_components_are_dropped_not_zeroed(self):
        settings = CurationSettings()
        with_all = keep_score(
            Signals(personal_probability=0.8, technical_quality=0.8), settings
        )
        personal_only = keep_score(Signals(personal_probability=0.8), settings)

        assert with_all is not None and personal_only is not None
        assert abs(with_all - personal_only) < 0.05

    def test_utility_lowers_the_score(self):
        settings = CurationSettings()
        clean = keep_score(Signals(personal_probability=0.9), settings)
        utility = keep_score(
            Signals(personal_probability=0.9, utility_probability=0.9), settings
        )

        assert clean > utility

    def test_no_positive_signal_gives_no_score(self):
        assert keep_score(Signals(utility_probability=0.4), CurationSettings()) is None

    def test_score_stays_within_range(self):
        settings = CurationSettings()
        score = keep_score(
            Signals(personal_probability=0.1, utility_probability=1.0), settings
        )

        assert score == 0.0


class TestConfidence:
    def test_grows_with_distance_from_the_threshold(self):
        settings = CurationSettings()
        signals = Signals(personal_probability=0.9, personal_source="semantic")

        near = confidence_for(0.71, signals, settings)
        far = confidence_for(0.98, signals, settings)

        assert far > near

    def test_uncalibrated_evidence_is_discounted(self):
        settings = CurationSettings()
        semantic = confidence_for(
            0.80,
            Signals(personal_probability=0.9, personal_source="semantic"),
            settings,
        )
        heuristic = confidence_for(
            0.80,
            Signals(personal_probability=0.9, personal_source="heuristic"),
            settings,
        )

        assert heuristic < semantic

    def test_no_score_means_no_confidence(self):
        assert confidence_for(None, Signals(), CurationSettings()) == 0.0


class TestSafetyRules:
    def test_a_confident_personal_photo_is_kept(self):
        verdict = fuse(
            {
                "personal_probability": 0.95,
                "utility_probability": 0.02,
                "technical_quality": 0.9,
            },
            ["personal_people"],
            [reasons.SEMANTIC_PERSONAL],
            CurationSettings(),
        )

        assert verdict.decision is CurationDecision.KEEP

    def test_conflicting_signals_go_to_review(self):
        """A person holding a receipt is exactly the case this protects."""
        verdict = fuse(
            {"personal_probability": 0.71, "utility_probability": 0.64},
            ["personal_people", "receipt_invoice"],
            [reasons.SEMANTIC_PERSONAL],
            CurationSettings(),
        )

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.SIGNAL_CONFLICT in verdict.reasons

    def test_a_protected_subject_blocks_an_automatic_reject(self):
        verdict = fuse(
            {
                "personal_probability": 0.30,
                "utility_probability": 0.50,
                "technical_quality": 0.2,
                "semantic.pet": 0.8,
            },
            ["document", "pet"],
            [reasons.SEMANTIC_UTILITY],
            CurationSettings(),
        )

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.PROTECTED_SUBJECT in verdict.reasons

    def test_a_certain_screenshot_overrides_the_protection(self):
        verdict = fuse(
            {
                "personal_probability": 0.2,
                "utility_probability": 0.95,
                "semantic.personal_people": 0.9,
            },
            ["screenshot", "personal_people"],
            [reasons.SEMANTIC_SCREENSHOT],
            CurationSettings(),
        )

        assert verdict.decision is CurationDecision.REJECT

    def test_reject_needs_semantic_evidence(self):
        """Metadata and blur alone may never discard a file."""
        verdict = fuse(
            {
                "rules.personal_prior": 0.2,
                "rules.utility_evidence": 0.8,
                "technical_quality": 0.1,
            },
            [],
            [reasons.LOW_SHARPNESS, reasons.PNG_WITHOUT_CAMERA_EXIF],
            CurationSettings(),
        )

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.NO_SEMANTIC_EVIDENCE in verdict.reasons

    def test_low_quality_alone_never_rejects(self):
        verdict = fuse(
            {"personal_probability": 0.8, "technical_quality": 0.01},
            ["portrait"],
            [reasons.SEMANTIC_PERSONAL, reasons.LOW_SHARPNESS],
            CurationSettings(),
        )

        assert verdict.decision is not CurationDecision.REJECT

    def test_a_score_inside_the_band_goes_to_review(self):
        verdict = fuse(
            {"personal_probability": 0.55, "technical_quality": 0.5},
            ["landscape"],
            [reasons.SEMANTIC_PERSONAL],
            CurationSettings(),
        )

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.SCORE_IN_BAND in verdict.reasons

    def test_a_failed_stage_forces_review(self):
        verdict = fuse(
            {"personal_probability": 0.1, "utility_probability": 0.9},
            ["screenshot"],
            [reasons.SEMANTIC_SCREENSHOT],
            CurationSettings(),
            stage_failed=True,
        )

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.STAGE_ERROR in verdict.reasons

    def test_a_failed_stage_also_blocks_a_confident_keep(self):
        """Unknown means review, and that has to hold for keeps too.

        This guarantee used to be delivered only as a side effect of the
        confidence floor, so it needs a case of its own.
        """
        verdict = fuse(
            {"personal_probability": 0.95, "technical_quality": 0.9},
            ["portrait"],
            [reasons.SEMANTIC_PERSONAL],
            CurationSettings(),
            stage_failed=True,
        )

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.STAGE_ERROR in verdict.reasons

    def test_no_signals_at_all_means_review(self):
        verdict = fuse({}, [], [], CurationSettings())

        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.MISSING_SIGNALS in verdict.reasons

    def test_a_borderline_keep_is_not_downgraded(self):
        """The keep threshold already encodes the margin a keep has to clear.

        Confidence is a function of that same distance, so re-checking it here
        would silently move the threshold the user configured.
        """
        settings = CurationSettings()
        verdict = fuse(
            {"personal_probability": 0.90, "technical_quality": 0.35},
            ["portrait"],
            [reasons.SEMANTIC_PERSONAL],
            settings,
        )

        assert verdict.keep_score >= settings.keep_threshold
        assert verdict.confidence < settings.minimum_confidence
        assert verdict.decision is CurationDecision.KEEP
        assert reasons.LOW_CONFIDENCE not in verdict.reasons

    def test_a_borderline_reject_is_downgraded_to_review(self):
        """The destructive direction still pays for the extra margin."""
        settings = CurationSettings()
        verdict = fuse(
            {"personal_probability": 0.32, "utility_probability": 0.10},
            ["screenshot"],
            [reasons.SEMANTIC_SCREENSHOT],
            settings,
        )

        assert verdict.keep_score <= settings.reject_threshold
        assert verdict.confidence < settings.minimum_confidence
        assert verdict.decision is CurationDecision.REVIEW
        assert reasons.LOW_CONFIDENCE in verdict.reasons


class TestHelpers:
    def test_protected_subject_needs_a_credible_score(self):
        assert has_protected_subject(["pet"], {"semantic.pet": 0.7})
        assert not has_protected_subject(["pet"], {"semantic.pet": 0.2})
        assert not has_protected_subject(["document"], {})

    def test_labels_map_to_reason_codes(self):
        assert semantic_reason_for("screenshot") == reasons.SEMANTIC_SCREENSHOT
        assert semantic_reason_for("landscape") == reasons.SEMANTIC_PERSONAL
        assert semantic_reason_for("receipt_invoice") == reasons.SEMANTIC_UTILITY

    def test_only_uncertain_reviews_are_escalated(self):
        settings = CurationSettings()
        uncertain = Verdict(CurationDecision.REVIEW, 0.5, 0.5, ())
        settled = Verdict(CurationDecision.KEEP, 0.9, 0.9, ())
        hopeless = Verdict(CurationDecision.REVIEW, 0.05, 0.5, ())

        assert needs_vlm_escalation(uncertain, settings)
        assert not needs_vlm_escalation(settled, settings)
        assert not needs_vlm_escalation(hopeless, settings)
