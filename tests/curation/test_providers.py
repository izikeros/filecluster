"""Tests for the provider contracts, fingerprints and pure helpers.

Nothing here loads model weights: the concrete providers are only checked for
their fingerprint behaviour, their lazy imports and their input validation.
"""

import numpy as np
import pytest

from filecluster.curation.configuration import CurationSettings
from filecluster.curation.exceptions import (
    MissingDependencyError,
    ProviderUnavailableError,
)
from filecluster.curation.provider_stages import OcrStage, SemanticStage
from filecluster.curation.providers.base import (
    OcrAggregates,
    ProviderInfo,
    Providers,
    SemanticPrediction,
    model_fingerprint,
)
from filecluster.curation.providers.ocr import text_density_evidence
from filecluster.curation.providers.preference import (
    MIN_EXAMPLES_PER_CLASS,
    LogisticPreferenceProvider,
    split_by_group,
)
from filecluster.curation.providers.semantic import load_prompt_bank
from filecluster.curation.providers.vlm import parse_vlm_response
from filecluster.curation.types import CurationDecision, SemanticLabel

from .conftest import (
    BrokenSemanticProvider,
    FakeOcrProvider,
    FakeSemanticProvider,
    make_item,
    write_photo,
)


class TestFingerprint:
    def test_no_providers_hash_to_a_stable_value(self):
        assert model_fingerprint(None) == model_fingerprint(Providers())

    def test_a_different_revision_changes_the_fingerprint(self):
        class Pinned(FakeSemanticProvider):
            def __init__(self, revision):
                super().__init__({"portrait": 0.5})
                self.revision = revision

            def info(self):
                return ProviderInfo(
                    name="semantic", model_id="fake", revision=self.revision
                )

        first = model_fingerprint(Providers(semantic=Pinned("a")))
        second = model_fingerprint(Providers(semantic=Pinned("b")))

        assert first != second

    def test_the_prompt_bank_version_is_part_of_the_fingerprint(self):
        base = ProviderInfo(name="semantic", model_id="m", prompt_bank_version="1")
        newer = ProviderInfo(name="semantic", model_id="m", prompt_bank_version="2")

        class Static:
            def __init__(self, info):
                self._info = info

            def info(self):
                return self._info

            def classify(self, images):
                return []

        assert model_fingerprint(Providers(semantic=Static(base))) != model_fingerprint(
            Providers(semantic=Static(newer))
        )


class TestSemanticStage:
    def test_maps_labels_onto_the_two_signals(self, tmp_path, settings, context):
        provider = FakeSemanticProvider(
            {"personal_people": 0.9, "document": 0.2, "receipt_invoice": 0.4}
        )
        context.set_image(object())
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = SemanticStage(provider, settings).analyze(item, context)

        assert result.scores["personal_probability"] == 0.9
        assert result.scores["utility_probability"] == 0.4
        assert result.labels[0] == "personal_people"

    def test_a_close_call_is_flagged(self, tmp_path, settings, context):
        provider = FakeSemanticProvider({"portrait": 0.61, "document": 0.60})
        context.set_image(object())
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = SemanticStage(provider, settings).analyze(item, context)

        assert "semantic.low_margin" in result.reasons

    def test_a_failure_is_contained(self, tmp_path, settings, context):
        context.set_image(object())
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = SemanticStage(BrokenSemanticProvider(), settings).analyze(
            item, context
        )

        assert result.failed
        assert result.terminal_decision is None

    def test_without_an_image_the_stage_reports_a_decode_error(
        self, tmp_path, settings, context
    ):
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = SemanticStage(FakeSemanticProvider({}), settings).analyze(
            item, context
        )

        assert result.failed
        assert "processing.decode_error" in result.reasons

    def test_the_provider_is_not_called_before_a_file_reaches_it(self, settings):
        """Weights must not load just because the stage was constructed."""
        provider = FakeSemanticProvider({"portrait": 0.5})

        SemanticStage(provider, settings)

        assert provider.calls == 0


class TestOcrStage:
    def test_reports_only_aggregates(self, tmp_path, context):
        provider = FakeOcrProvider(
            OcrAggregates(
                blocks=12,
                lines=12,
                characters=400,
                mean_confidence=0.9,
                text_area_fraction=0.4,
            )
        )
        context.set_image(np.zeros((10, 10, 3), dtype="uint8"))
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = OcrStage(provider).analyze(item, context)

        assert "ocr.text_density_evidence" in result.scores
        assert all(isinstance(v, float) for v in result.scores.values())


class TestTextDensity:
    def test_no_text_means_no_evidence(self):
        assert text_density_evidence(OcrAggregates()) == 0.0

    def test_dense_confident_text_is_strong_evidence(self):
        aggregates = OcrAggregates(
            blocks=40,
            lines=40,
            characters=2000,
            mean_confidence=0.95,
            text_area_fraction=0.5,
        )

        assert text_density_evidence(aggregates) > 0.8

    def test_low_confidence_dampens_the_signal(self):
        confident = OcrAggregates(lines=20, mean_confidence=0.9, text_area_fraction=0.4)
        unsure = OcrAggregates(lines=20, mean_confidence=0.2, text_area_fraction=0.4)

        assert text_density_evidence(unsure) < text_density_evidence(confident)


class TestPromptBank:
    def test_the_packaged_bank_covers_every_label(self):
        bank = load_prompt_bank()

        missing = {label.value for label in SemanticLabel} - set(bank.labels)
        assert missing == set()

    def test_prompts_and_owners_stay_parallel(self):
        texts, owners = load_prompt_bank().flat()

        assert len(texts) == len(owners)

    def test_a_broken_bank_fails_loudly(self, tmp_path):
        path = tmp_path / "prompts.json"
        path.write_text('{"labels": {}}', encoding="utf-8")

        with pytest.raises(ProviderUnavailableError):
            load_prompt_bank(path)


class TestVlmValidation:
    def test_accepts_a_well_formed_answer(self):
        judgement = parse_vlm_response(
            '{"decision": "keep", "confidence": 0.82, '
            '"labels": ["personal_people"], "reasons": ["people are the subject"]}'
        )

        assert judgement.decision is CurationDecision.KEEP
        assert judgement.confidence == 0.82
        assert judgement.labels == ("personal_people",)

    def test_invalid_json_is_refused(self):
        with pytest.raises(ProviderUnavailableError):
            parse_vlm_response("not json at all")

    def test_an_unknown_decision_is_refused(self):
        with pytest.raises(ProviderUnavailableError):
            parse_vlm_response('{"decision": "delete"}')

    def test_unknown_labels_are_dropped(self):
        judgement = parse_vlm_response(
            '{"decision": "review", "labels": ["portrait", "../../etc/passwd"]}'
        )

        assert judgement.labels == ("portrait",)

    def test_confidence_is_clamped(self):
        assert (
            parse_vlm_response('{"decision": "keep", "confidence": 9}').confidence
            == 1.0
        )

    def test_reasons_are_capped(self):
        judgement = parse_vlm_response(
            {"decision": "keep", "reasons": ["x" * 500, "a", "b", "c", "d", "e"]}
        )

        assert len(judgement.reasons) <= 4
        assert all(len(reason) <= 200 for reason in judgement.reasons)


class TestPreferenceModel:
    def test_untrained_model_refuses_to_score(self):
        with pytest.raises(ValueError, match="not been trained"):
            LogisticPreferenceProvider().score([0.1, 0.2])

    def test_training_needs_enough_examples(self):
        rng = np.random.default_rng(0)
        embeddings = rng.normal(size=(20, 8))
        labels = [1] * 10 + [0] * 10

        with pytest.raises(ValueError, match=str(MIN_EXAMPLES_PER_CLASS)):
            LogisticPreferenceProvider().fit(embeddings, labels, ["g"] * 20)

    def test_learns_a_separable_signal(self):
        rng = np.random.default_rng(1)
        keep = rng.normal(1.0, 0.3, size=(120, 6))
        reject = rng.normal(-1.0, 0.3, size=(120, 6))
        embeddings = np.vstack([keep, reject])
        labels = [1] * 120 + [0] * 120
        groups = [f"event-{i // 10}" for i in range(240)]

        provider = LogisticPreferenceProvider()
        report = provider.fit(embeddings, labels, groups)

        assert report.validation_accuracy > 0.9
        assert provider.score(keep[0]) > provider.score(reject[0])

    def test_state_survives_a_round_trip(self):
        provider = LogisticPreferenceProvider(weights=[0.5, -0.5], bias=0.1)
        restored = LogisticPreferenceProvider.from_state_dict(provider.state_dict())

        assert restored.score([1.0, 0.0]) == provider.score([1.0, 0.0])

    def test_embedding_size_is_validated(self):
        provider = LogisticPreferenceProvider(weights=[0.5, -0.5])

        with pytest.raises(ValueError, match="dimensions"):
            provider.score([1.0, 0.0, 3.0])


class TestGroupSplit:
    def test_no_group_appears_on_both_sides(self):
        """Photos from one burst must not straddle the split."""
        groups = [f"event-{i // 5}" for i in range(50)]

        train, validation = split_by_group(groups, 0.3, seed=3)

        train_groups = {groups[i] for i in train}
        validation_groups = {groups[i] for i in validation}
        assert train_groups & validation_groups == set()
        assert len(train) + len(validation) == 50

    def test_the_split_is_reproducible(self):
        groups = [f"e{i // 4}" for i in range(40)]

        assert split_by_group(groups, seed=7) == split_by_group(groups, seed=7)


class TestOptionalDependencies:
    def test_unimplemented_providers_explain_how_to_install(self):
        from filecluster.curation.providers.quality import AestheticProvider
        from filecluster.curation.providers.vlm import LocalVlmProvider

        for factory in (AestheticProvider, LocalVlmProvider):
            with pytest.raises(MissingDependencyError) as exc:
                factory()
            assert "pip install" in str(exc.value)

    def test_semantic_provider_reports_a_missing_extra(self, monkeypatch):
        import filecluster.curation.providers.semantic as semantic

        monkeypatch.setattr(semantic, "semantic_extra_available", lambda: False)

        with pytest.raises(MissingDependencyError):
            semantic.SigLipSemanticProvider()

    def test_prediction_ranking_is_ordered(self):
        prediction = SemanticPrediction(scores={"a": 0.1, "b": 0.9, "c": 0.5})

        assert prediction.ranked(2) == (("b", 0.9), ("c", 0.5))

    def test_settings_control_which_providers_are_expected(self):
        assert not CurationSettings().enable_semantic
