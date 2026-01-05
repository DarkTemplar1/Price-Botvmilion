#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETAP 1 – czyszczenie adresów

WYMAGANIA (SPEŁNIONE):
- wejście może być CAŁE Z DUŻYCH LITER (np. PŁOCKIE)
- stare województwa (49) -> nowe (16)
- PŁOCKIE -> MAZOWIECKIE (OBUSTRONNIE UPPERCASE)
- --- / brak / nan -> None
- wynik ZAWSZE UPPERCASE
- zapis NADPISUJE TYLKO arkusz 'raport'
"""

from __future__ import annotations
import argparse
from pathlib import Path
import re
import unicodedata
import pandas as pd


# ============================================================
# KONFIGURACJA
# ============================================================

RAPORT_SHEET = "raport"

ADMIN_COLS = ["Województwo", "Powiat", "Gmina", "Miejscowość"]

HINT_COLS = [
    "Cały adres (dla lokalu)",
    "Położenie",
    "Ulica(dla lokalu)",
    "Ulica(dla budynku)",
    "Ulica",
    "Dzielnica",
    "Miejscowość",
]

MISSING_TOKENS = {
    "---", "--", "—", "-", "brak", "brak danych", "nan", "none", ""
}

# ============================================================
# MAPA HISTORYCZNA WOJEWÓDZTW (1975–1998 -> 1999+)
# KLUCZE: po norm_key() (bez ogonków, lower)
# WARTOŚCI: ZAWSZE UPPERCASE
# ============================================================

HISTORICAL_VOIVODESHIP_MAP = {
    # DOLNOŚLĄSKIE
    "wroclawskie": "DOLNOŚLĄSKIE",
    "jeleniogorskie": "DOLNOŚLĄSKIE",
    "walbrzyskie": "DOLNOŚLĄSKIE",
    "legnickie": "DOLNOŚLĄSKIE",

    # KUJAWSKO-POMORSKIE
    "bydgoskie": "KUJAWSKO-POMORSKIE",
    "torunskie": "KUJAWSKO-POMORSKIE",
    "wloclawskie": "KUJAWSKO-POMORSKIE",

    # LUBELSKIE
    "lubelskie": "LUBELSKIE",
    "bialskopodlaskie": "LUBELSKIE",
    "chelmskie": "LUBELSKIE",
    "zamojskie": "LUBELSKIE",

    # LUBUSKIE
    "zielonogorskie": "LUBUSKIE",
    "gorzowskie": "LUBUSKIE",

    # ŁÓDZKIE
    "lodzkie": "ŁÓDZKIE",
    "piotrkowskie": "ŁÓDZKIE",
    "sieradzkie": "ŁÓDZKIE",
    "skierniewickie": "ŁÓDZKIE",

    # MAŁOPOLSKIE
    "krakowskie": "MAŁOPOLSKIE",
    "tarnowskie": "MAŁOPOLSKIE",
    "nowosadeckie": "MAŁOPOLSKIE",

    # MAZOWIECKIE
    "warszawskie": "MAZOWIECKIE",
    "plockie": "MAZOWIECKIE",
    "ciechanowskie": "MAZOWIECKIE",
    "ostroleckie": "MAZOWIECKIE",
    "siedleckie": "MAZOWIECKIE",
    "radomskie": "MAZOWIECKIE",

    # OPOLSKIE
    "opolskie": "OPOLSKIE",

    # PODKARPACKIE
    "rzeszowskie": "PODKARPACKIE",
    "przemyskie": "PODKARPACKIE",
    "krosnienskie": "PODKARPACKIE",
    "tarnobrzeskie": "PODKARPACKIE",

    # PODLASKIE
    "bialostockie": "PODLASKIE",
    "lomzynskie": "PODLASKIE",
    "suwalskie": "PODLASKIE",

    # POMORSKIE
    "gdanskie": "POMORSKIE",
    "slupskie": "POMORSKIE",

    # ŚLĄSKIE
    "katowickie": "ŚLĄSKIE",
    "bielskie": "ŚLĄSKIE",
    "czestochowskie": "ŚLĄSKIE",

    # ŚWIĘTOKRZYSKIE
    "kieleckie": "ŚWIĘTOKRZYSKIE",

    # WARMIŃSKO-MAZURSKIE
    "olsztynskie": "WARMIŃSKO-MAZURSKIE",
    "elblaskie": "WARMIŃSKO-MAZURSKIE",

    # WIELKOPOLSKIE
    "poznanskie": "WIELKOPOLSKIE",
    "kaliskie": "WIELKOPOLSKIE",
    "koninskie": "WIELKOPOLSKIE",
    "leszczynskie": "WIELKOPOLSKIE",
    "pilskie": "WIELKOPOLSKIE",

    # ZACHODNIOPOMORSKIE
    "szczecinskie": "ZACHODNIOPOMORSKIE",
    "koszalinskie": "ZACHODNIOPOMORSKIE",
}

# warianty "WOJEWÓDZTWO X"
for k, v in list(HISTORICAL_VOIVODESHIP_MAP.items()):
    HISTORICAL_VOIVODESHIP_MAP[f"wojewodztwo {k}"] = v
    HISTORICAL_VOIVODESHIP_MAP[f"{k} wojewodztwo"] = v


# ============================================================
# HELPERY
# ============================================================

def norm_missing(x):
    if x is None:
        return None
    s = str(x).strip()
    return None if s.lower() in MISSING_TOKENS else s


def norm_key(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip().lower()
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def replace_historical_voivodeship(val: str | None) -> str | None:
    """
    PŁOCKIE -> MAZOWIECKIE
    WOJEWÓDZTWO PŁOCKIE -> MAZOWIECKIE
    """
    val = norm_missing(val)
    if val is None:
        return None

    key = norm_key(val)
    if not key:
        return None

    if key in HISTORICAL_VOIVODESHIP_MAP:
        return HISTORICAL_VOIVODESHIP_MAP[key]

    key2 = key.replace("wojewodztwo ", "").strip()
    if key2 in HISTORICAL_VOIVODESHIP_MAP:
        return HISTORICAL_VOIVODESHIP_MAP[key2]

    return val.upper()


def build_addr_hint(row: pd.Series) -> str | None:
    parts = []
    for c in HINT_COLS:
        if c in row.index:
            v = norm_missing(row[c])
            if v:
                parts.append(str(v))
    return " | ".join(parts).upper() if parts else None


# ============================================================
# IO – NADPISUJEMY TYLKO ARKUSZ "raport"
# ============================================================

def read_df(xlsx: Path) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx, engine="openpyxl")
    sheet = RAPORT_SHEET if RAPORT_SHEET in xl.sheet_names else xl.sheet_names[0]
    return pd.read_excel(xlsx, sheet_name=sheet, engine="openpyxl")


def write_replace_raport(xlsx: Path, df: pd.DataFrame):
    with pd.ExcelWriter(
        xlsx,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df.to_excel(writer, sheet_name=RAPORT_SHEET, index=False)


# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raport", help="Plik XLSX")
    args = ap.parse_args()

    raport = Path(args.raport)
    df = read_df(raport)

    # normalizacja kolumn administracyjnych
    for col in ADMIN_COLS:
        if col in df.columns:
            df[col] = df[col].map(norm_missing).map(lambda x: x.upper() if x else None)

    # zamiana historycznych województw
    if "Województwo" in df.columns:
        df["Województwo"] = df["Województwo"].apply(replace_historical_voivodeship)

    # hint
    df["_addr_hint"] = df.apply(build_addr_hint, axis=1)

    # zapis
    write_replace_raport(raport, df)

    print("✔ ETAP 1 OK – PŁOCKIE → MAZOWIECKIE, wszystko UPPERCASE")


if __name__ == "__main__":
    main()
