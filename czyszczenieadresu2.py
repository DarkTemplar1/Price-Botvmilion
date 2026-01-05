#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETAP 2 – uzupełnianie Woj/Pow/Gmi/Miejscowość na podstawie:
- teryt.csv (Wojewodztwo;Powiat;Gmina;Miejscowosc;Dzielnica)
- obszar_sadow.xlsx (Oznaczenie sądu + jednostki admin)

Założenia:
- w raporcie braki mogą być zapisane jako '---' -> traktujemy jak brak
- wykorzystujemy:
  * istniejące kolumny raportu
  * _addr_hint (jeśli dodany w ETAP 1)
  * 'Nr KW' -> wyciągamy kod sądu (np. WR1K)
"""

from __future__ import annotations
import argparse
import re
import unicodedata
from pathlib import Path
import pandas as pd


# =========================
# KONFIG / kolumny raportu
# =========================

RAPORT_SHEET = "raport"

COL_WOJ = "Województwo"
COL_POW = "Powiat"
COL_GMI = "Gmina"
COL_MIA = "Miejscowość"

COL_KW = "Nr KW"

# źródła do “hintów adresowych” (im więcej, tym lepiej)
HINT_COLS = [
    "_addr_hint",
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


# =========================
# NORMALIZACJA
# =========================

def norm_missing(x):
    if x is None:
        return None
    s = str(x).strip()
    return None if s.lower() in MISSING_TOKENS else s


def norm_key(s: str | None) -> str:
    """do porównań: bez ogonków, lower, pojedyncze spacje"""
    s = str(s or "")
    s = s.strip().lower()
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


def title_pl(s: str | None) -> str:
    """ładniejszy zapis nazwy (nie jest idealny, ale wystarczy do raportu)"""
    s = str(s or "").strip()
    if not s:
        return ""
    return s[:1].upper() + s[1:]


# =========================
# KW -> kod sądu
# =========================

KW_COURT_RE = re.compile(r"^\s*([A-Z]{2}\d[A-Z])\s*/", re.I)

def extract_court_code(nr_kw: str | None) -> str | None:
    """
    Przykład KW: WR1K/00012345/6 -> kod sądu: WR1K
    """
    if not nr_kw:
        return None
    m = KW_COURT_RE.match(str(nr_kw).strip().upper())
    return m.group(1) if m else None


# =========================
# Hint adresowy -> kandydat miejscowości
# =========================

SPLIT_RE = re.compile(r"[|,;]+")

def build_hint_text(row: pd.Series) -> str:
    parts = []
    for c in HINT_COLS:
        if c in row.index:
            v = norm_missing(row[c])
            if v:
                parts.append(str(v))
    return " | ".join(parts)


def guess_miejscowosc_from_hint(hint: str, miejsc_keys_set: set[str], miejsc_key_to_canon: dict[str, str]) -> str | None:
    """
    Heurystyka:
    1) sprawdź segmenty po przecinku / | (często końcówka to miasto)
    2) sprawdź n-gramy 1..4 słowa z całego hintu (żeby złapać np. 'Nowy Sącz')
    Zwraca kanoniczną nazwę miejscowości (z TERYT), jeśli znajdzie.
    """
    h = norm_key(hint)
    if not h:
        return None

    # 1) segmenty (od końca) — np. "ul. X, Warszawa"
    segs = [norm_key(s) for s in SPLIT_RE.split(hint) if norm_key(s)]
    for seg in reversed(segs):
        seg = seg.strip()
        # obetnij typowe przedrostki
        seg = re.sub(r"^(ul|ulica|al|aleja|os|osiedle|pl|plac)\.?\s+", "", seg).strip()
        if seg in miejsc_keys_set:
            return miejsc_key_to_canon[seg]

    # 2) n-gramy słów
    words = [w for w in re.split(r"\s+", h) if w]
    # skanuj dłuższe najpierw
    for n in (4, 3, 2, 1):
        if len(words) < n:
            continue
        for i in range(0, len(words) - n + 1):
            phrase = " ".join(words[i:i+n]).strip()
            if phrase in miejsc_keys_set:
                return miejsc_key_to_canon[phrase]

    return None


# =========================
# Budowa indeksów: TERYT + OBSZAR SĄDÓW
# =========================

def load_teryt(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";", encoding="utf-8", engine="python")
    # ujednolić typy
    for c in ["Wojewodztwo", "Powiat", "Gmina", "Miejscowosc", "Dzielnica"]:
        if c in df.columns:
            df[c] = df[c].astype(str).map(lambda x: x.strip() if x and x != "nan" else "")
    return df


def load_obszar(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    # spodziewane kolumny:
    # Oznaczenie sądu, Województwo, Powiat, Gmina, Miejscowość, Dzielnica
    return df


def build_teryt_index(teryt: pd.DataFrame):
    """
    indeks miejscowość -> lista rekordów (woj/pow/gmi)
    oraz set kluczy do szybkiego matchowania.
    """
    miejsc_key_to_canon: dict[str, str] = {}
    miejsc_to_rows: dict[str, list[tuple[str, str, str]]] = {}

    for _, r in teryt.iterrows():
        miejsc = (r.get("Miejscowosc") or "").strip()
        woj = (r.get("Wojewodztwo") or "").strip()
        powiat = (r.get("Powiat") or "").strip()
        gmina = (r.get("Gmina") or "").strip()

        if not miejsc:
            continue

        k = norm_key(miejsc)
        if not k:
            continue

        # kanon: taka jak w pliku teryt (zwykle wielkie litery)
        # do raportu zrobimy ładniej title_pl()
        miejsc_key_to_canon.setdefault(k, miejsc)

        miejsc_to_rows.setdefault(k, []).append((woj, powiat, gmina))

    miejsc_keys_set = set(miejsc_key_to_canon.keys())
    return miejsc_keys_set, miejsc_key_to_canon, miejsc_to_rows


def build_obszar_index(obs: pd.DataFrame):
    """
    indeks:
    - court_code -> unique woj (jeśli jednoznaczne)
    - court_code -> miejscowość_key -> (woj, pow, gmi, miejscowość)
    """
    court_to_woj: dict[str, str] = {}
    court_to_miejsc: dict[str, dict[str, tuple[str, str, str, str]]] = {}

    if "Oznaczenie sądu" not in obs.columns:
        return court_to_woj, court_to_miejsc

    for code, g in obs.groupby("Oznaczenie sądu"):
        code = str(code).strip().upper()
        # województwo bywa stałe dla sądu
        woj_vals = [str(x).strip() for x in g.get("Województwo", pd.Series()).dropna().unique().tolist()]
        woj_vals = [w for w in woj_vals if w and w.lower() != "nan"]
        if len(set(woj_vals)) == 1:
            court_to_woj[code] = woj_vals[0]

        m_map: dict[str, tuple[str, str, str, str]] = {}
        for _, r in g.iterrows():
            miejsc = str(r.get("Miejscowość") or "").strip()
            if not miejsc or miejsc.lower() == "nan":
                continue
            k = norm_key(miejsc)
            woj = str(r.get("Województwo") or "").strip()
            powiat = str(r.get("Powiat") or "").strip()
            gmina = str(r.get("Gmina") or "").strip()
            # preferuj pierwsze wystąpienie
            m_map.setdefault(k, (woj, powiat, gmina, miejsc))
        court_to_miejsc[code] = m_map

    return court_to_woj, court_to_miejsc


# =========================
# Uzupełnianie
# =========================

def fill_from_teryt(miejsc_key: str, wanted_woj: str | None, miejsc_to_rows: dict[str, list[tuple[str, str, str]]]):
    """
    Zwraca (woj, powiat, gmina) na podstawie miejscowości.
    Jeśli wanted_woj podany, to wybieramy rekordy z tym woj.
    """
    rows = miejsc_to_rows.get(miejsc_key, [])
    if not rows:
        return None, None, None

    if wanted_woj:
        w = norm_key(wanted_woj)
        filtered = [r for r in rows if norm_key(r[0]) == w]
        if filtered:
            rows = filtered

    woj, powiat, gmina = rows[0]
    return woj or None, powiat or None, gmina or None


def main():
    ap = argparse.ArgumentParser(description="ETAP 2 – uzupełnianie adresów z teryt.csv + obszar_sadow.xlsx")
    ap.add_argument("raport", help="Plik raportu .xlsx/.xlsm")
    ap.add_argument("--teryt", default="teryt.csv", help="Ścieżka do teryt.csv")
    ap.add_argument("--obszar", default="obszar_sadow.xlsx", help="Ścieżka do obszar_sadow.xlsx")
    ap.add_argument("--sheet", default=RAPORT_SHEET, help="Nazwa arkusza (domyślnie: raport)")
    args = ap.parse_args()

    raport = Path(args.raport).resolve()
    teryt_path = Path(args.teryt).resolve()
    obszar_path = Path(args.obszar).resolve()

    if not raport.exists():
        raise FileNotFoundError(f"Brak raportu: {raport}")
    if not teryt_path.exists():
        raise FileNotFoundError(f"Brak teryt.csv: {teryt_path}")
    if not obszar_path.exists():
        raise FileNotFoundError(f"Brak obszar_sadow.xlsx: {obszar_path}")

    # --- wczytaj raport ---
    xl = pd.ExcelFile(raport, engine="openpyxl")
    sheet = args.sheet if args.sheet in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(raport, sheet_name=sheet, engine="openpyxl")

    # --- wczytaj źródła ---
    teryt = load_teryt(teryt_path)
    obs = load_obszar(obszar_path)

    miejsc_keys_set, miejsc_key_to_canon, miejsc_to_rows = build_teryt_index(teryt)
    court_to_woj, court_to_miejsc = build_obszar_index(obs)

    # upewnij się, że kolumny istnieją
    for c in [COL_WOJ, COL_POW, COL_GMI, COL_MIA]:
        if c not in df.columns:
            df[c] = None

    # normalizacja braków w docelowych kolumnach
    for c in [COL_WOJ, COL_POW, COL_GMI, COL_MIA]:
        df[c] = df[c].map(norm_missing)

    if COL_KW in df.columns:
        df[COL_KW] = df[COL_KW].map(lambda x: str(x).strip() if pd.notna(x) else "")

    filled_cnt = 0
    filled_woj_cnt = 0
    filled_pow_cnt = 0
    filled_gmi_cnt = 0
    filled_mia_cnt = 0

    for i in range(len(df)):
        row = df.iloc[i]

        woj = norm_missing(row.get(COL_WOJ))
        powiat = norm_missing(row.get(COL_POW))
        gmina = norm_missing(row.get(COL_GMI))
        miejsc = norm_missing(row.get(COL_MIA))

        # kontekst z KW -> sąd
        court_code = extract_court_code(row.get(COL_KW)) if COL_KW in df.columns else None

        # jeśli brak woj, a mamy sąd i jest jednoznaczny woj w obszar_sadow
        if not woj and court_code and court_code in court_to_woj:
            woj = court_to_woj[court_code]
            df.at[i, COL_WOJ] = title_pl(woj)
            filled_woj_cnt += 1

        # jeśli brak miejscowości, spróbuj wyciągnąć z hintu (TERYT)
        hint = build_hint_text(row)
        if not miejsc:
            guess = guess_miejscowosc_from_hint(hint, miejsc_keys_set, miejsc_key_to_canon)
            if guess:
                miejsc = guess
                df.at[i, COL_MIA] = title_pl(miejsc)
                filled_mia_cnt += 1

        # jeśli dalej brak miejscowości, spróbuj dopasować w obrębie sądu (obszar_sadow)
        if not miejsc and court_code and court_code in court_to_miejsc:
            m_map = court_to_miejsc[court_code]
            # spróbuj wyłuskać nazwę miejscowości z hintu i sprawdzić czy jest w tym sądzie
            # (ta sama funkcja, tylko inny słownik)
            # budujemy “mini-mapę” dla sądu:
            local_keys_set = set(m_map.keys())
            local_key_to_canon = {k: v[3] for k, v in m_map.items()}
            guess_local = guess_miejscowosc_from_hint(hint, local_keys_set, local_key_to_canon)
            if guess_local:
                miejsc = guess_local
                df.at[i, COL_MIA] = title_pl(miejsc)
                filled_mia_cnt += 1
                # a przy okazji możemy podstawić pow/gmi jeśli brak
                k = norm_key(miejsc)
                woj_o, pow_o, gmi_o, _ = m_map.get(k, ("", "", "", ""))
                if not woj and woj_o:
                    df.at[i, COL_WOJ] = title_pl(woj_o)
                    filled_woj_cnt += 1
                    woj = woj_o
                if not powiat and pow_o:
                    df.at[i, COL_POW] = title_pl(pow_o)
                    filled_pow_cnt += 1
                    powiat = pow_o
                if not gmina and gmi_o:
                    df.at[i, COL_GMI] = title_pl(gmi_o)
                    filled_gmi_cnt += 1
                    gmina = gmi_o

        # jeśli mamy miejscowość, to uzupełnij powiat/gminę z TERYT
        if miejsc:
            mk = norm_key(miejsc)
            woj_from_teryt, pow_from_teryt, gmi_from_teryt = fill_from_teryt(
                mk,
                wanted_woj=woj,
                miejsc_to_rows=miejsc_to_rows
            )

            if not woj and woj_from_teryt:
                df.at[i, COL_WOJ] = title_pl(woj_from_teryt)
                filled_woj_cnt += 1
                woj = woj_from_teryt
            if not powiat and pow_from_teryt:
                df.at[i, COL_POW] = title_pl(pow_from_teryt)
                filled_pow_cnt += 1
                powiat = pow_from_teryt
            if not gmina and gmi_from_teryt:
                df.at[i, COL_GMI] = title_pl(gmi_from_teryt)
                filled_gmi_cnt += 1
                gmina = gmi_from_teryt

        # policz jako “uzupełnione wiersze” jeśli cokolwiek dodaliśmy w tym kroku
        # (prosty licznik)
        # Tu: heurystycznie – jeśli po operacjach mamy więcej pól niż na początku:
        after = (
            bool(df.at[i, COL_WOJ]) +
            bool(df.at[i, COL_POW]) +
            bool(df.at[i, COL_GMI]) +
            bool(df.at[i, COL_MIA])
        )
        before = (
            bool(row.get(COL_WOJ)) +
            bool(row.get(COL_POW)) +
            bool(row.get(COL_GMI)) +
            bool(row.get(COL_MIA))
        )
        if after > before:
            filled_cnt += 1

    # zapis: replace tylko arkusza, reszta arkuszy zostaje
    with pd.ExcelWriter(raport, engine="openpyxl", mode="a", if_sheet_exists="replace") as wr:
        df.to_excel(wr, sheet_name=sheet, index=False)

    print("✔ ETAP 2 ZAKOŃCZONY")
    print(f"  raport: {raport}")
    print(f"  arkusz: {sheet}")
    print(f"  wiersze z poprawą: {filled_cnt}")
    print(f"  uzupełniono: woj={filled_woj_cnt}, pow={filled_pow_cnt}, gmi={filled_gmi_cnt}, miejsc={filled_mia_cnt}")


if __name__ == "__main__":
    main()
