import pytest
import string
import re
from dtb.utils.name import generate_random_alphanumeric


class TestRandomAlphanumeric:
    def test_length_accuracy(self):
        """Test that generated strings have the correct length"""
        test_lengths = [1, 5, 10, 100]
        for length in test_lengths:
            result = generate_random_alphanumeric(length)
            assert len(result) == length

    def test_character_set(self):
        """Test that generated strings only contain allowed characters"""
        # Generate a long string to ensure good character coverage
        result = generate_random_alphanumeric(1000)
        allowed_chars = set(string.ascii_letters + string.digits)

        # Check each character is in the allowed set
        for char in result:
            assert char in allowed_chars

    def test_randomness(self):
        """Test that generated strings are random and unique"""
        # Generate multiple strings of the same length
        length = 10
        samples = [generate_random_alphanumeric(length) for _ in range(100)]

        # Check uniqueness (it's extremely unlikely to get duplicates with length 10)
        unique_samples = set(samples)
        assert len(unique_samples) > 90  # Allow for extremely rare collisions

    def test_character_distribution(self):
        """Test that all character types (uppercase, lowercase, digits) are present"""
        # Generate a long string to ensure good distribution
        result = generate_random_alphanumeric(1000)

        # Check presence of each character type
        has_uppercase = bool(re.search(r"[A-Z]", result))
        has_lowercase = bool(re.search(r"[a-z]", result))
        has_digits = bool(re.search(r"[0-9]", result))

        assert has_uppercase, "Missing uppercase letters"
        assert has_lowercase, "Missing lowercase letters"
        assert has_digits, "Missing digits"

    def test_zero_length(self):
        """Test behavior with zero length"""
        with pytest.raises(ValueError):
            generate_random_alphanumeric(0)

    def test_negative_length(self):
        """Test behavior with negative length"""
        with pytest.raises(ValueError):
            generate_random_alphanumeric(-1)

    def test_large_length(self):
        """Test behavior with large length"""
        length = 10000
        result = generate_random_alphanumeric(length)
        assert len(result) == length

    def test_non_integer_length(self):
        """Test behavior with non-integer length"""
        invalid_lengths = [1.5, "5", None, [5]]
        for length in invalid_lengths:
            with pytest.raises((TypeError, ValueError)):
                generate_random_alphanumeric(length)

    @pytest.mark.parametrize("length", [1, 10, 100])
    def test_multiple_calls_different_results(self, length):
        """Test that multiple calls with same length produce different results"""
        result1 = generate_random_alphanumeric(length)
        result2 = generate_random_alphanumeric(length)
        assert result1 != result2

    def test_character_counts(self):
        """Test that character counts are roughly evenly distributed"""
        # Generate a very long string to minimize random variation
        result = generate_random_alphanumeric(10000)

        # Count each character type
        uppercase_count = sum(1 for c in result if c in string.ascii_uppercase)
        lowercase_count = sum(1 for c in result if c in string.ascii_lowercase)
        digit_count = sum(1 for c in result if c in string.digits)

        # Calculate expected ranges (allowing for random variation)
        total_chars = len(string.ascii_letters + string.digits)
        expected_uppercase = len(result) * len(string.ascii_uppercase) / total_chars
        expected_lowercase = len(result) * len(string.ascii_lowercase) / total_chars
        expected_digits = len(result) * len(string.digits) / total_chars

        # Allow for 20% deviation from expected values
        assert abs(uppercase_count - expected_uppercase) / expected_uppercase < 0.2
        assert abs(lowercase_count - expected_lowercase) / expected_lowercase < 0.2
        assert abs(digit_count - expected_digits) / expected_digits < 0.2

    def test_seeded_randomness(self):
        """Test that seeding random produces consistent results"""
        import random

        # Set seed
        random.seed(42)
        result1 = generate_random_alphanumeric(10)

        # Reset seed
        random.seed(42)
        result2 = generate_random_alphanumeric(10)

        assert result1 == result2
