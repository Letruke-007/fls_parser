#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


TWOPLACES = Decimal("0.01")

RAW_HEADERS = [
    "Месяц",
    "Год",
    "Сод.жил.пом.",
    "Наем",
    "Отоп.осн.пл.",
    "ГВ",
    "ХВ",
    "ХВ для ГВ",
    "Канализация",
    "Радио и Оповещение",
    "Антенна",
    "Запирающее устройство",
    "Газ",
    "Взнос на кап. ремонт",
    "Обращ. с ТКО",
    "Другие услуги",
    "Всего",
    "Корр-ка",
    "Оплачено",
    "Итого задол-ть",
]

HEADER_MAP = {
    "месяц": "month",
    "год": "year",
    "сод.жил.пом.": "maintenance_housing",
    "наем": "rent",
    "hаем": "rent",
    "отоп.осн.пл.": "heating_main",
    "гв": "hot_water",
    "хв": "cold_water",
    "хв для гв": "cold_water_for_hot_water",
    "канализация": "sewerage",
    "радио и оповещение": "radio_and_alert",
    "антенна": "antenna",
    "запирающее устройство": "locking_device",
    "газ": "gas",
    "взнос на кап. ремонт": "capital_repair",
    "обращ. с тко": "solid_waste",
    "другие услуги": "other_services",
    "всего": "total_accrued",
    "корр-ка": "adjustment",
    "оплачено": "paid",
    "итого задол-ть": "debt_total",
}

SERVICE_FIELDS = [
    "maintenance_housing",
    "rent",
    "heating_main",
    "hot_water",
    "cold_water",
    "cold_water_for_hot_water",
    "sewerage",
    "radio_and_alert",
    "antenna",
    "locking_device",
    "gas",
    "capital_repair",
    "solid_waste",
    "other_services",
]

TOTAL_FIELDS = SERVICE_FIELDS + ["total_accrued", "adjustment", "paid", "debt_total"]


@dataclass
class ParseState:
    text: str
    index: int = 0
    ucskip: int = 1
    skip: int = 0
    ignorable: bool = False


def _normalize_spaces(text: str) -> str:
    return " ".join(
        text.replace("\u00a0", " ")
        .replace("\u202f", " ")
        .replace("\u2007", " ")
        .strip()
        .split()
    )


def _norm_header(text: str) -> str:
    return _normalize_spaces(text).lower()


def rtf_to_text(data: bytes) -> str:
    cp_match = re.search(br"\\ansicpg(\d+)", data[:4096])
    encoding = f"cp{cp_match.group(1).decode()}" if cp_match else "cp1251"
    text = data.decode(encoding, errors="ignore")
    state = ParseState(text=text)
    stack: List[Tuple[int, int, bool]] = []
    out: List[str] = []
    destinations = {
        "fonttbl",
        "colortbl",
        "datastore",
        "themedata",
        "stylesheet",
        "info",
        "pict",
        "object",
        "xmlnstbl",
        "header",
        "footer",
        "headerl",
        "headerr",
        "footerl",
        "footerr",
    }
    specialchars = {
        "par": "\n",
        "line": "\n",
        "row": "\n",
        "tab": "\t",
        "cell": "\t",
        "emdash": "-",
        "endash": "-",
        "bullet": "*",
        "lquote": "'",
        "rquote": "'",
        "ldblquote": '"',
        "rdblquote": '"',
    }

    while state.index < len(state.text):
        ch = state.text[state.index]
        state.index += 1

        if state.skip > 0:
            state.skip -= 1
            continue

        if ch == "{":
            stack.append((state.ucskip, state.skip, state.ignorable))
            continue

        if ch == "}":
            if stack:
                state.ucskip, state.skip, state.ignorable = stack.pop()
            continue

        if ch != "\\":
            if not state.ignorable:
                out.append(ch)
            continue

        if state.index >= len(state.text):
            break

        nxt = state.text[state.index]
        if nxt in "\\{}":
            if not state.ignorable:
                out.append(nxt)
            state.index += 1
            continue

        if nxt == "*":
            state.ignorable = True
            state.index += 1
            continue

        if nxt == "'":
            hex_code = state.text[state.index + 1 : state.index + 3]
            if len(hex_code) == 2:
                try:
                    if not state.ignorable:
                        out.append(bytes.fromhex(hex_code).decode(encoding, errors="ignore"))
                except Exception:
                    pass
            state.index += 3
            continue

        ctrl_match = re.match(r"([a-zA-Z]+)(-?\d+)? ?", state.text[state.index :])
        if not ctrl_match:
            state.index += 1
            continue

        word = ctrl_match.group(1)
        arg = ctrl_match.group(2)
        state.index += len(ctrl_match.group(0))

        if word in destinations:
            state.ignorable = True
        elif word in specialchars:
            if not state.ignorable:
                out.append(specialchars[word])
        elif word == "u" and arg is not None:
            num = int(arg)
            if num < 0:
                num += 65536
            if not state.ignorable:
                out.append(chr(num))
            state.skip = state.ucskip
        elif word == "uc" and arg is not None:
            state.ucskip = int(arg)
        elif word == "bin" and arg is not None:
            state.skip = int(arg)

    return "".join(out)


def _clean_lines(text: str) -> List[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return [_normalize_spaces(line) for line in normalized.split("\n") if _normalize_spaces(line)]


def _tokenize(text: str) -> List[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ").replace("\n", " ")
    return [token for token in re.split(r"\s+", normalized) if token]


def _clean_joined_text(tokens: Iterable[str]) -> str:
    text = " ".join(tokens)
    text = re.sub(r"\s+([,.:;])", r"\1", text)
    text = re.sub(r"([№])\s+", r"\1", text)
    text = re.sub(r"ул\s+\.", "ул.", text)
    text = re.sub(r"кв\s+\.", "кв.", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _alpha_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalpha())


def _is_table_header_line(line: str, next_line: Optional[str] = None) -> bool:
    key = _alpha_key(line)
    if key.startswith("месяц"):
        return True
    if key == "ме" and next_line:
        return _alpha_key(next_line).startswith("сяц")
    return False


def _strip_table_tail_from_address(value: str) -> str:
    normalized = normalize_whitespace(value)
    patterns = (
        r"\bМе\s*сяц\b.*$",
        r"\bМесяц\b.*$",
        r"\bГод\s+Сод\b.*$",
    )
    for pattern in patterns:
        normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)
    return normalized.strip(" ,;:")


def _extract_header_fields(lines: List[str]) -> Tuple[Optional[str], Optional[str]]:
    header_lines: List[str] = []
    for idx, line in enumerate(lines):
        next_line = lines[idx + 1] if idx + 1 < len(lines) else None
        if _is_table_header_line(line, next_line):
            break
        header_lines.append(line)

    header_text = " ".join(header_lines)
    header_text = normalize_whitespace(header_text)
    match = re.search(
        r"Ф\s*\.?\s*И\s*\.?\s*О\s*\.?\s*(?P<name>.+?)\s*Адрес\s*:?\s*(?P<address>.+)$",
        header_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None, None

    account_holder_name = normalize_account_holder_name(match.group("name").strip(" ,;:"))
    address_raw = normalize_address_ocr_noise(_strip_table_tail_from_address(match.group("address")))
    return account_holder_name or None, address_raw or None


def normalize_cyrillic_lookalikes(value: str) -> str:
    if not re.search(r"[А-Яа-яЁё]", value) or not re.search(r"[A-Za-z]", value):
        return value
    mapping = str.maketrans(
        {
            "A": "А",
            "a": "а",
            "B": "В",
            "C": "С",
            "c": "с",
            "E": "Е",
            "e": "е",
            "H": "Н",
            "K": "К",
            "M": "М",
            "O": "О",
            "o": "о",
            "P": "Р",
            "p": "р",
            "T": "Т",
            "X": "Х",
            "x": "х",
            "Y": "У",
            "y": "у",
        }
    )
    return value.translate(mapping)


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _merge_split_cyrillic_word_tokens(value: str) -> str:
    normalized = normalize_whitespace(value)
    previous = None
    while normalized != previous:
        previous = normalized
        normalized = re.sub(
            r"\b([А-Яа-яЁё-]{3,})\s+([А-Яа-яЁё])\b",
            r"\1\2",
            normalized,
        )
    return normalized


def _looks_like_patronymic(value: str) -> bool:
    lowered = value.lower()
    return lowered.endswith(("вич", "вна", "ична", "инична", "оглы", "кызы"))


def is_likely_female_name(tokens: List[str]) -> bool:
    surname = tokens[0] if tokens else ""
    first_name = tokens[1] if len(tokens) > 1 else ""
    if surname.endswith(("ова", "ева", "ина", "ая")):
        return True
    return first_name.endswith(("а", "я", "на"))


def fix_broken_patronymic_token(value: str, is_female: bool) -> Optional[str]:
    stripped = value.strip("-")
    if len(stripped) < 3:
        return None

    replacements = (
        ("ьеб", "ьевна" if is_female else "ьевич"),
        ("еб", "евна" if is_female else "евич"),
        ("об", "овна" if is_female else "ович"),
        ("б", "вна" if is_female else "вич"),
    )
    lowered = stripped.lower()
    for bad_suffix, good_suffix in replacements:
        if lowered.endswith(bad_suffix):
            base = stripped[: -len(bad_suffix)]
            if base:
                return f"{base}{good_suffix}"
    return None


def normalize_account_holder_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return value

    normalized = normalize_cyrillic_lookalikes(normalize_whitespace(value))
    normalized = re.sub(r"([а-яё])([А-ЯЁ])", r"\1 \2", normalized)
    tokens = normalized.split()

    if len(tokens) == 4 and len(tokens[2]) <= 2:
        merged_patronymic = f"{tokens[2]}{tokens[3]}"
        if _looks_like_patronymic(merged_patronymic):
            tokens = [tokens[0], tokens[1], merged_patronymic]

    if len(tokens) == 3:
        fixed_patronymic = fix_broken_patronymic_token(tokens[2], is_likely_female_name(tokens))
        if fixed_patronymic:
            tokens[2] = fixed_patronymic

    return " ".join(tokens)


def normalize_address_ocr_noise(value: str) -> str:
    normalized = normalize_cyrillic_lookalikes(normalize_whitespace(value))
    normalized = re.sub(r"\bд\s+ом\b", "дом", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bк\s+в\b", "кв", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bк\s+\.\s*(?=\d)", "к. ", normalized, flags=re.IGNORECASE)
    normalized = _merge_split_cyrillic_word_tokens(normalized)
    return normalized


def normalize_for_parsing(text: str) -> str:
    normalized = normalize_address_ocr_noise(text)
    normalized = re.sub(r"\b[uU][лЛ]\s*\.?\s*", "ул. ", normalized)
    normalized = re.sub(r"\bul\s*\.?\s*", "ул. ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\b[KК][BВ]\s*\.?\s*", "кв. ", normalized)
    normalized = re.sub(r"\b[кК]\s*\.?\s*(?P<value>\d[\w/-]*)", r"корп. \g<value>", normalized)
    normalized = normalized.replace("дом.", "дом")
    normalized = normalized.replace("дом №", "дом ")
    normalized = normalized.replace("дом Ng", "дом ")
    normalized = normalized.replace("дом No", "дом ")
    normalized = normalized.replace("д.", "дом ")
    normalized = normalized.replace("кор.", "корп.")
    normalized = normalized.replace("кор:", "корп. ")
    normalized = normalized.replace("к.", "корп. ")
    normalized = normalized.replace("стр.", "строение ")
    normalized = normalized.replace("стр:", "строение ")
    normalized = normalized.replace("ул:", "ул. ")
    normalized = re.sub(r"\s+,", ",", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip(" ,")
    return normalized


def normalize_street(value: str) -> str:
    street = normalize_whitespace(value).strip(",")
    street = normalize_cyrillic_lookalikes(street)
    street = re.sub(r"^(.+?)\s+ул(?:ица)?\.?$", r"ул. \1", street, flags=re.IGNORECASE)
    street = re.sub(r"^(.+?)\s+бульвар\.?$", r"б-р \1", street, flags=re.IGNORECASE)
    street = re.sub(r"^(.+?)\s+просп(?:ект)?\.?$", r"пр-кт \1", street, flags=re.IGNORECASE)
    street = re.sub(r"^(.+?)\s+переулок\.?$", r"пер. \1", street, flags=re.IGNORECASE)
    street = re.sub(r"\b[uU][лЛ]\.?", "ул.", street)
    street = re.sub(r"\bul\.\b", "ул.", street, flags=re.IGNORECASE)
    street = re.sub(r"^ul\.\s+", "ул. ", street, flags=re.IGNORECASE)
    street = re.sub(r"^uл\.\s+", "ул. ", street, flags=re.IGNORECASE)
    street = street.rstrip(":;,")
    street = re.sub(r"\bул\s+\.", "ул.", street, flags=re.IGNORECASE)
    street = re.sub(r"\bбульв\.?\b", "бульвар", street, flags=re.IGNORECASE)
    street = re.sub(r"^ул\.\s+(.+?)\s+бульвар$", r"б-р \1", street, flags=re.IGNORECASE)
    street = re.sub(r"^ул\.\s+(.+?)\s+просп\.?$", r"пр-кт \1", street, flags=re.IGNORECASE)
    patterns = (
        (r"^(?:ул(?:ица)?\.?\s+)?(.+?)\s+ул(?:ица)?\.?[:;,]?$", "ул. {body}"),
        (r"^(?:пр(?:-?кт|осп(?:ект)?)\.?\s+)?(.+?)\s+(?:пр(?:-?кт|осп(?:ект)?)\.?)$", "пр-кт {body}"),
        (r"^(?:б-р\s+)?(.+?)\s+бульвар\.?$", "б-р {body}"),
        (r"^(?:пер(?:еулок)?\.?\s+)?(.+?)\s+пер(?:еулок)?\.?$", "пер. {body}"),
        (r"^(?:ш(?:оссе)?\.?\s+)?(.+?)\s+шоссе\.?$", "ш. {body}"),
        (r"^(?:пр(?:оезд)?\.?\s+)?(.+?)\s+проезд\.?$", "пр. {body}"),
    )
    for pattern, template in patterns:
        match = re.match(pattern, street, flags=re.IGNORECASE)
        if match:
            street = template.format(body=match.group(1))
            break
    street = re.sub(r"^ул(?:ица)?\.?\s+", "ул. ", street, flags=re.IGNORECASE)
    street = re.sub(r"^ул\.\s+ул\.\s+", "ул. ", street, flags=re.IGNORECASE)
    street = re.sub(r"^просп(?:ект)?\.?\s+", "пр-кт ", street, flags=re.IGNORECASE)
    street = re.sub(r"^пр-кт\s+пр-кт\s+", "пр-кт ", street, flags=re.IGNORECASE)
    street = re.sub(r"\s+,", ",", street)
    street = normalize_whitespace(street).rstrip(":;,")
    return normalize_whitespace(street)


def clean_optional_token(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip().strip(" ,;:.")
    cleaned = re.sub(r"(?<=\d)[A](?=$)", "А", cleaned)
    cleaned = re.sub(r"(?<=\d)[a](?=$)", "а", cleaned)
    if not cleaned or set(cleaned) == {"_"}:
        return None
    if _norm_header(cleaned) in {"корп", "строение", "кв", "дом"}:
        return None
    return cleaned


def build_public_address(
    street: Optional[str],
    house: Optional[str],
    building: Optional[str],
    structure: Optional[str],
    apartment: Optional[str],
) -> Optional[str]:
    parts: List[str] = []
    if street:
        parts.append(street)
    if house:
        parts.append(f"дом № {house}")
    if building:
        parts.append(f"корп. {building}")
    if structure:
        parts.append(f"строение {structure}")
    if apartment:
        parts.append(f"кв. {apartment}")
    return ", ".join(parts) if parts else None


def parse_address(raw: Optional[str]) -> Dict[str, Optional[str]]:
    address = {
        "raw": raw,
        "street": None,
        "house": None,
        "building": None,
        "structure": None,
        "apartment": None,
        "full": None,
    }
    if not raw:
        return address

    normalized = normalize_for_parsing(raw)
    apartment = clean_optional_token(
        next((m.group("value") for m in [
            re.search(r"кв\s*\.?\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
            re.search(r"квартира\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
        ] if m), None)
    )
    structure = clean_optional_token(
        next((m.group("value") for m in [
            re.search(r"строение\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
            re.search(r"\bстр\.?\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
        ] if m), None)
    )
    building = clean_optional_token(
        next((m.group("value") for m in [
            re.search(r"корп\.?\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
            re.search(r"корпус\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
        ] if m), None)
    )
    house = clean_optional_token(
        next((m.group("value") for m in [
            re.search(r"дом\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
            re.search(r"\bд\s*\.?\s*(?P<value>[\w/-]+)", normalized, flags=re.IGNORECASE),
        ] if m), None)
    )

    street_part = normalized
    for pattern in (
        r"(?:,\s*|\s+)дом\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)д\s*\.?\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)корп\.?\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)корпус\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)строение\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)\bстр\.?\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)кв\s*\.?\s*[\w/-]+.*$",
        r"(?:,\s*|\s+)квартира\s*[\w/-]+.*$",
    ):
        street_part = re.sub(pattern, "", street_part, flags=re.IGNORECASE)
    street_part = street_part.strip(" ,;:")
    street = None
    if street_part:
        explicit_patterns = (
            (r"^(?P<body>.+?)\s+ул\.?$", "ул. {body}"),
            (r"^(?P<body>.+?)\s+бульвар\.?$", "б-р {body}"),
            (r"^(?P<body>.+?)\s+просп(?:ект)?\.?$", "пр-кт {body}"),
            (r"^(?P<body>.+?)\s+переулок\.?$", "пер. {body}"),
        )
        for pattern, template in explicit_patterns:
            match = re.match(pattern, street_part, flags=re.IGNORECASE)
            if match:
                street = normalize_whitespace(template.format(body=match.group("body")))
                break
        if street is None:
            street = normalize_street(street_part)

    address.update(
        {
            "street": street,
            "house": house,
            "building": building,
            "structure": structure,
            "apartment": apartment,
        }
    )
    address["full"] = build_public_address(street, house, building, structure, apartment)
    return address


def _parse_decimal(token: str) -> Decimal:
    clean = token.replace(" ", "").replace(",", ".")
    return Decimal(clean).quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def _to_json_number(value: Optional[Decimal]) -> Optional[float | int]:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _is_month(token: str) -> bool:
    return bool(re.fullmatch(r"\d{2}", token))


def _is_year(token: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", token))


def _is_date(token: str) -> bool:
    return bool(re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", token))


def _canonical_headers(headers: Iterable[str]) -> List[str]:
    return [HEADER_MAP[_norm_header(header)] for header in headers]


def _read_row(tokens: List[str], index: int, width: int) -> Tuple[List[str], int]:
    return tokens[index : index + width], index + width


def _is_numeric_amount_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d+(?:,\d+)?", token))


def _merge_split_amount_tokens(left: str, right: str) -> Optional[str]:
    if not re.fullmatch(r"\d+", left):
        return None
    if not _is_numeric_amount_token(right):
        return None
    merged = f"{left}{right}"
    try:
        _parse_decimal(merged)
    except Exception:
        return None
    return merged


def _score_charge_row(row: Dict[str, Any]) -> Tuple[int, Decimal]:
    service_sum = sum((row[field] for field in SERVICE_FIELDS), Decimal("0.00")).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    debt_expected = (row["total_accrued"] + row["adjustment"] - row["paid"]).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    passes = int(row["total_accrued"] == service_sum) + int(row["debt_total"] == debt_expected)
    delta = abs(row["total_accrued"] - service_sum) + abs(row["debt_total"] - debt_expected)
    return passes, delta.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def _is_row_boundary_token(token: str) -> bool:
    return (
        _is_month(token)
        or token in {"Итого", "Всего"}
        or token.startswith("Исполнитель:")
        or token == "Исполнитель"
        or token == ":"
        or token.lower().startswith("pirmodule")
        or _is_date(token)
    )


def _normalize_table_tokens(tokens: List[str]) -> List[str]:
    normalized: List[str] = []
    idx = 0
    while idx < len(tokens):
        if idx + 1 < len(tokens) and tokens[idx] == "Всег" and tokens[idx + 1] == "о":
            normalized.append("Всего")
            idx += 2
            continue
        normalized.append(tokens[idx])
        idx += 1
    return normalized


def _read_charge_row(tokens: List[str], index: int, headers: List[str]) -> Tuple[List[str], int]:
    width = len(headers)
    row_tokens, next_index = _read_row(tokens, index, width)
    if next_index >= len(tokens) or _is_row_boundary_token(tokens[next_index]):
        return row_tokens, next_index

    if next_index + 1 > len(tokens):
        return row_tokens, next_index

    extended = tokens[index : index + width + 1]
    if len(extended) != width + 1:
        return row_tokens, next_index

    try:
        best_row = _row_to_charge(row_tokens, headers)
        best_score = _score_charge_row(best_row)
        best_tokens = row_tokens
        best_next_index = next_index
    except Exception:
        best_score = (-1, Decimal("Infinity"))
        best_tokens = row_tokens
        best_next_index = next_index

    for merge_idx in range(2, len(extended) - 1):
        merged = _merge_split_amount_tokens(extended[merge_idx], extended[merge_idx + 1])
        if merged is None:
            continue
        candidate = extended[:merge_idx] + [merged] + extended[merge_idx + 2 :]
        if len(candidate) != width:
            continue
        try:
            row = _row_to_charge(candidate, headers)
        except Exception:
            continue
        score = _score_charge_row(row)
        if score > best_score:
            best_score = score
            best_tokens = candidate
            best_next_index = index + width + 1

    return best_tokens, best_next_index


def _row_to_charge(row_tokens: List[str], headers: List[str]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for header, token in zip(headers, row_tokens):
        if header in {"month", "year"}:
            row[header] = token
        else:
            row[header] = _parse_decimal(token)
    return row


def _row_to_total(row_tokens: List[str], headers: List[str], include_year: bool) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    row["row_type"] = row_tokens[0]
    shift = 1
    if include_year:
        row["year"] = row_tokens[1]
        shift = 2
    for header, token in zip(headers[2:], row_tokens[shift:]):
        row[header] = _parse_decimal(token)
    return row


def _sum_fields(rows: Iterable[Dict[str, Any]], fields: Iterable[str]) -> Dict[str, Decimal]:
    sums = {field: Decimal("0.00") for field in fields}
    for row in rows:
        for field in fields:
            value = row.get(field)
            if isinstance(value, Decimal):
                sums[field] += value
    return {field: value.quantize(TWOPLACES, rounding=ROUND_HALF_UP) for field, value in sums.items()}


def _make_check(
    scope: str,
    rule: str,
    passed: bool,
    actual: Decimal,
    expected: Decimal,
    **extra: Any,
) -> Dict[str, Any]:
    delta = (actual - expected).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    payload = {
        "scope": scope,
        "rule": rule,
        "passed": passed,
        "actual": _to_json_number(actual),
        "expected": _to_json_number(expected),
        "delta": _to_json_number(delta),
    }
    payload.update(extra)
    return payload


def validate_statement(
    charges: List[Dict[str, Any]],
    year_totals: List[Dict[str, Any]],
    grand_total: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    for idx, row in enumerate(charges, start=1):
        service_sum = sum((row[field] for field in SERVICE_FIELDS), Decimal("0.00")).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
        checks.append(
            _make_check(
                scope="charge",
                rule="total_accrued_equals_sum_of_services",
                passed=row["total_accrued"] == service_sum,
                actual=row["total_accrued"],
                expected=service_sum,
                row_index=idx,
                period=f"{row['month']}.{row['year']}",
            )
        )

        debt_expected = (row["total_accrued"] + row["adjustment"] - row["paid"]).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
        checks.append(
            _make_check(
                scope="charge",
                rule="debt_total_equals_total_plus_adjustment_minus_paid",
                passed=row["debt_total"] == debt_expected,
                actual=row["debt_total"],
                expected=debt_expected,
                row_index=idx,
                period=f"{row['month']}.{row['year']}",
            )
        )

    for total in year_totals:
        year = total["year"]
        matching = [row for row in charges if row["year"] == year]
        aggregated = _sum_fields(matching, TOTAL_FIELDS)
        for field in TOTAL_FIELDS:
            checks.append(
                _make_check(
                    scope="year_total",
                    rule="year_total_matches_sum_of_monthly_rows",
                    passed=total[field] == aggregated[field],
                    actual=total[field],
                    expected=aggregated[field],
                    year=year,
                    field=field,
                )
            )

    if grand_total is not None:
        aggregated = _sum_fields(charges, TOTAL_FIELDS)
        for field in TOTAL_FIELDS:
            checks.append(
                _make_check(
                    scope="grand_total",
                    rule="grand_total_matches_sum_of_monthly_rows",
                    passed=grand_total[field] == aggregated[field],
                    actual=grand_total[field],
                    expected=aggregated[field],
                    field=field,
                )
            )

    passed_count = sum(1 for check in checks if check["passed"])
    return {
        "is_valid": passed_count == len(checks),
        "checks_total": len(checks),
        "checks_passed": passed_count,
        "checks_failed": len(checks) - passed_count,
        "checks": checks,
    }


def parse_statement(path: Path) -> Dict[str, Any]:
    text = rtf_to_text(path.read_bytes())
    lines = _clean_lines(text)
    tokens = _tokenize(text)
    headers = _canonical_headers(RAW_HEADERS)

    account_holder_name, address_raw = _extract_header_fields(lines)
    header_start = None
    fio_start = None
    address_marker = None
    for idx in range(len(tokens) - 2):
        probe = "".join(ch for ch in "".join(tokens[idx : idx + 3]) if ch.isalpha()).lower()
        if fio_start is None and (probe == "фио" or probe.startswith("фио")):
            fio_start = idx
        if address_marker is None and tokens[idx].startswith("Адрес"):
            address_marker = idx
        if header_start is None:
            joined = "".join(tokens[idx : idx + 2]).lower()
            if joined.startswith("месяц") or tokens[idx].lower().startswith("месяц"):
                header_start = idx
        if fio_start is not None and address_marker is not None and header_start is not None:
            break

    if header_start is None:
        raise RuntimeError("Не удалось распознать шапку документа")

    if account_holder_name is None or address_raw is None:
        if fio_start is None or address_marker is None:
            raise RuntimeError("Не удалось распознать шапку документа")
        account_holder_name = _clean_joined_text(tokens[fio_start + 1 : address_marker]) or None
        account_holder_name = re.sub(r"^[ОO]\.?", "", account_holder_name or "").strip() or None
        account_holder_name = normalize_account_holder_name(account_holder_name)
        address_raw = _clean_joined_text(tokens[address_marker + 1 : header_start]) or None
        address_raw = normalize_address_ocr_noise(address_raw) if address_raw else None

    address = parse_address(address_raw)

    data_start = None
    for idx in range(header_start, len(tokens) - 1):
        if _is_month(tokens[idx]) and _is_year(tokens[idx + 1]):
            data_start = idx
            break

    if data_start is None:
        raise RuntimeError("Не удалось найти начало табличных данных")

    headers_raw = RAW_HEADERS[:]
    tokens = _normalize_table_tokens(tokens[data_start:])
    charges: List[Dict[str, Any]] = []
    year_totals: List[Dict[str, Any]] = []
    grand_total: Optional[Dict[str, Any]] = None
    parsing_warnings: List[str] = []

    idx = 0
    while idx < len(tokens):
        token = tokens[idx]

        if _is_month(token):
            if idx + len(headers) > len(tokens):
                parsing_warnings.append(f"Оборванная месячная строка у токена {idx + 1}")
                break
            row_tokens, idx = _read_charge_row(tokens, idx, headers)
            if not _is_year(row_tokens[1]):
                raise RuntimeError(f"После месяца '{row_tokens[0]}' ожидался год")
            charges.append(_row_to_charge(row_tokens, headers))
            continue

        if token == "Итого":
            if idx + len(headers) > len(tokens):
                parsing_warnings.append(f"Оборванная строка итога у токена {idx + 1}")
                break
            row_tokens, idx = _read_row(tokens, idx, len(headers))
            if not _is_year(row_tokens[1]):
                raise RuntimeError("После строки 'Итого' ожидался год")
            year_totals.append(_row_to_total(row_tokens, headers, include_year=True))
            continue

        if token == "Всего":
            needed = len(headers) - 1
            if idx + needed > len(tokens):
                parsing_warnings.append(f"Оборванная общая итоговая строка у токена {idx + 1}")
                break
            row_tokens, idx = _read_row(tokens, idx, needed)
            grand_total = _row_to_total(row_tokens, headers, include_year=False)
            continue

        if token in {"Исполнитель", ":"} or token.startswith("Исполнитель:") or token.lower().startswith("pirmodule") or _is_date(token):
            idx += 1
            continue

        parsing_warnings.append(f"Неожиданный токен '{token}' на позиции {idx + 1}")
        idx += 1

    validations = validate_statement(charges, year_totals, grand_total)

    def serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for key, value in row.items():
            payload[key] = _to_json_number(value) if isinstance(value, Decimal) else value
        return payload

    return {
        "document_type": "account_statement",
        "statement_title": "Выписка из лицевого счета о задолженности по квартплате и коммунальным услугам",
        "source_filename": path.name,
        "account_holder_name": account_holder_name,
        "address_raw": address_raw,
        "address": address,
        "charges": [serialize_row(row) for row in charges],
        "year_totals": [serialize_row(row) for row in year_totals],
        "grand_total": serialize_row(grand_total) if grand_total else None,
        "validations": validations,
        "parsing": {
            "headers_raw": headers_raw,
            "warnings": parsing_warnings,
        },
    }
