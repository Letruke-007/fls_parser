#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from app.parser import _extract_header_fields, normalize_account_holder_name, parse_address


class ParseAddressTests(unittest.TestCase):
    def test_preserves_apartment_when_abbreviation_has_space_before_dot(self) -> None:
        result = parse_address("Донецкая ул., дом 22, кв . 1")

        self.assertEqual(result["street"], "ул. Донецкая")
        self.assertEqual(result["house"], "22")
        self.assertEqual(result["apartment"], "1")
        self.assertEqual(result["full"], "ул. Донецкая, дом № 22, кв. 1")

    def test_restores_house_number_when_dom_word_is_split(self) -> None:
        result = parse_address("Маршала Голованова ул., д ом 12, кв. 95")

        self.assertEqual(result["street"], "ул. Маршала Голованова")
        self.assertEqual(result["house"], "12")
        self.assertEqual(result["apartment"], "95")
        self.assertEqual(result["full"], "ул. Маршала Голованова, дом № 12, кв. 95")

    def test_restores_split_street_word(self) -> None:
        result = parse_address("Новочеркасски й бульв., дом 15, кв . 104")

        self.assertEqual(result["street"], "б-р Новочеркасский")
        self.assertEqual(result["house"], "15")
        self.assertEqual(result["apartment"], "104")
        self.assertEqual(result["full"], "б-р Новочеркасский, дом № 15, кв. 104")

    def test_does_not_parse_structure_from_locking_device_word(self) -> None:
        result = parse_address("Перерва ул., дом 6, кв. 92 Запирающее устройство")

        self.assertEqual(result["street"], "ул. Перерва")
        self.assertEqual(result["house"], "6")
        self.assertIsNone(result["structure"])
        self.assertEqual(result["apartment"], "92")
        self.assertEqual(result["full"], "ул. Перерва, дом № 6, кв. 92")


class ExtractHeaderFieldsTests(unittest.TestCase):
    def test_stops_before_split_month_header(self) -> None:
        lines = [
            "ВЫПИСКА",
            "Ф",
            ".И.",
            "О.",
            "Васильев",
            "Петр",
            "Иванович",
            "Адрес:",
            "Перерва",
            "ул.,",
            "дом 6,",
            "кв. 92",
            "Ме",
            "сяц Год",
            "Сод.",
        ]

        name, address = _extract_header_fields(lines)

        self.assertEqual(name, "Васильев Петр Иванович")
        self.assertEqual(address, "Перерва ул., дом 6, кв. 92")


class NormalizeAccountHolderNameTests(unittest.TestCase):
    def test_restores_split_patronymic(self) -> None:
        self.assertEqual(
            normalize_account_holder_name("Гусева Анна В икторовна"),
            "Гусева Анна Викторовна",
        )

    def test_restores_two_letter_patronymic_prefix(self) -> None:
        self.assertEqual(
            normalize_account_holder_name("Мороз Анна Ан атольевна"),
            "Мороз Анна Анатольевна",
        )


if __name__ == "__main__":
    unittest.main()
