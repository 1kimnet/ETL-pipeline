"""Unit tests for etl.utils.naming module."""
import pytest

from etl.utils.naming import (
    sanitize_for_filename,
    sanitize_for_arcgis_name,
    generate_fc_name
)


class TestSanitizeForFilename:
    """Test sanitize_for_filename function."""

    @pytest.mark.unit
    def test_basic_filename_sanitization(self):
        assert sanitize_for_filename("Hello World") == "hello_world"

    @pytest.mark.unit
    def test_filename_with_swedish_chars(self):
        assert sanitize_for_filename("Åland Äpplen") == "aland_applen"

    @pytest.mark.unit
    def test_filename_with_special_chars(self):
        assert sanitize_for_filename("Test@#$Data") == "test_data"


class TestSanitizeForArcgisName:
    """Test sanitize_for_arcgis_name function."""

    @pytest.mark.unit
    def test_basic_arcgis_sanitization(self):
        assert sanitize_for_arcgis_name("Hello World") == "Hello_World"

    @pytest.mark.unit
    def test_arcgis_hyphens_to_underscores(self):
        assert sanitize_for_arcgis_name(
            "Hello-World-Test") == "Hello_World_Test"

    @pytest.mark.unit
    def test_arcgis_swedish_characters(self):
        assert sanitize_for_arcgis_name("Åland Äpplen") == "aland_applen"

    @pytest.mark.unit
    def test_arcgis_starts_with_digit(self):
        assert sanitize_for_arcgis_name("123Test") == "_123test"

    @pytest.mark.unit
    def test_arcgis_consecutive_underscores(self):
        assert sanitize_for_arcgis_name("Hello___World") == "hello_world"

    @pytest.mark.unit
    def test_arcgis_leading_trailing_underscores(self):
        assert sanitize_for_arcgis_name("_Hello_World_") == "hello_world"

    @pytest.mark.unit
    def test_arcgis_special_characters_removed(self):
        assert sanitize_for_arcgis_name("Hello@#$World!") == "hello_world"

    @pytest.mark.unit
    def test_arcgis_empty_string(self):
        assert sanitize_for_arcgis_name("") == "unnamed"

    @pytest.mark.unit
    def test_arcgis_max_length_truncation(self):
        long_name = "a" * 150  # Longer than 128 chars
        result = sanitize_for_arcgis_name(long_name)
        assert len(result) <= 128
        assert result == "a" * 128

    @pytest.mark.unit
    def test_arcgis_real_world_examples(self):
        assert sanitize_for_arcgis_name(
            "Naturvårdsverket") == "naturvardsverket"
        assert sanitize_for_arcgis_name(
            "Försvarsmakten - Geodata") == "forsvarsmakten_geodata"
        assert sanitize_for_arcgis_name(
            "SGU-Berggrund 1:50 000") == "sgu_berggrund_1_50_000"


class TestGenerateFcName:
    """Test generate_fc_name function."""

    @pytest.mark.unit
    def test_basic_fc_name_generation(self):
        result = generate_fc_name("TEST", "Sample Data")
        assert result == "TEST_sample_data"

    @pytest.mark.unit
    def test_fc_name_with_swedish_chars(self):
        result = generate_fc_name("NVV", "Naturvårdsområden")
        assert result == "NVV_naturvardsomraden"

    @pytest.mark.unit
    def test_fc_name_with_special_chars(self):
        result = generate_fc_name("SGU", "Berggrund 1:50 000")
        assert result == "SGU_berggrund_1_50_000"

    @pytest.mark.unit
    def test_fc_name_max_length(self):
        long_source = "Very Long Source Name That Exceeds Normal Limits"
        result = generate_fc_name("AUTHORITY", long_source)
        assert len(result) <= 128
        assert not result.endswith("_")

    @pytest.mark.unit
    def test_fc_name_empty_source(self):
        result = generate_fc_name("TEST", "")
        assert result == "TEST_unnamed"

    @pytest.mark.unit
    def test_fc_name_strips_trailing_underscore(self):
        # Test that trailing underscores are properly stripped
        result = generate_fc_name("TEST", "Source___")
        assert not result.endswith("_")
        assert result == "TEST_source"

    @pytest.mark.unit
    def test_fc_name_real_world_examples(self):
        # Test with actual authority/source combinations
        assert generate_fc_name(
            "FM", "Rikstäckande geodata") == "FM_rikstackande_geodata"
        assert generate_fc_name(
            "NVV", "Naturvårdsregistret") == "NVV_naturvardsregistret"
        assert generate_fc_name(
            "SGU", "Berggrundskarta 1:50 000") == "SGU_berggrundskarta_1_50_000"

    @pytest.mark.unit
    def test_fc_name_authority_with_numbers(self):
        result = generate_fc_name("AUTH123", "Test Data")
        assert result == "AUTH123_test_data"
