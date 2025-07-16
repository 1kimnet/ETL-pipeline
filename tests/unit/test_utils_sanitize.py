"""Unit tests for etl.utils.sanitize module."""
import pytest

from etl.utils.sanitize import slugify


class TestSlugify:
    """Test the slugify function."""
    
    @pytest.mark.unit
    def test_slugify_basic(self):
        assert slugify("Hello World") == "hello_world"
    
    @pytest.mark.unit
    def test_slugify_swedish_characters(self):
        assert slugify("Åland Äpplen Öppna") == "aland_applen_oppna"
        assert slugify("ÅLAND ÄPPLEN ÖPPNA") == "aland_applen_oppna"
    
    @pytest.mark.unit
    def test_slugify_special_characters(self):
        assert slugify("Hello-World!@#$%") == "hello_world"
    
    @pytest.mark.unit
    def test_slugify_multiple_spaces(self):
        assert slugify("Hello   World    Test") == "hello_world_test"
    
    @pytest.mark.unit
    def test_slugify_leading_trailing_spaces(self):
        assert slugify("  Hello World  ") == "hello_world"
    
    @pytest.mark.unit
    def test_slugify_numbers(self):
        assert slugify("Test123 Data456") == "test123_data456"
    
    @pytest.mark.unit
    def test_slugify_hyphens_preserved_then_converted(self):
        # Based on the code, hyphens are kept in regex but then converted later
        assert slugify("Hello-World-Test") == "hello_world_test"
    
    @pytest.mark.unit
    def test_slugify_underscores_preserved(self):
        assert slugify("Hello_World_Test") == "hello_world_test"
    
    @pytest.mark.unit
    def test_slugify_empty_string(self):
        assert slugify("") == "unnamed"
    
    @pytest.mark.unit
    def test_slugify_whitespace_only(self):
        assert slugify("   ") == "unnamed"
    
    @pytest.mark.unit
    def test_slugify_special_chars_only(self):
        assert slugify("!@#$%^&*()") == "unnamed"
    
    @pytest.mark.unit
    def test_slugify_consecutive_underscores_collapsed(self):
        assert slugify("Hello____World") == "hello_world"
    
    @pytest.mark.unit
    def test_slugify_mixed_case_with_swedish(self):
        assert slugify("TeSt ÅäÖ DaTa") == "test_aao_data"
    
    @pytest.mark.unit
    def test_slugify_real_world_examples(self):
        # Test with realistic source names
        assert slugify("Naturvårdsverket - Naturvårdsregistret") == "naturvardsverket_naturvardsregistret"
        assert slugify("Försvarsmakten - Rikstäckande geodata") == "forsvarsmakten_rikstackande_geodata"
        assert slugify("SGU - Berggrundskarta 1:50 000") == "sgu_berggrundskarta_1_50_000"