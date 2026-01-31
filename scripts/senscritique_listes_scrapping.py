import re
import time
import argparse
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin

import requests
from bs4 import BeautifulSoup


@dataclass
class ListEntry:
    title: str
    year: Optional[int]
    film_url: str
    user_rating: Optional[str]      # ex: "6/10"
    annotation: Optional[str]       # commentaire de la liste (bloc "Annotation :")


class SensCritiqueListScraper:
    USER_AGENT = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    )

    def __init__(self, sleep_s: float = 1.2, timeout_s: float = 20.0):
        self.sleep_s = sleep_s
        self.timeout_s = timeout_s
        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "User-Agent": self.USER_AGENT,
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            }
        )

    @staticmethod
    def _set_page(url: str, page: int) -> str:
        """Force ?page=<n> sur une URL."""
        u = urlparse(url)
        q = parse_qs(u.query)
        q["page"] = [str(page)]
        new_query = urlencode(q, doseq=True)
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

    def _fetch_soup(self, url: str) -> BeautifulSoup:
        r = self.sess.get(url, timeout=self.timeout_s)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    @staticmethod
    def _extract_year_from_title(raw: str) -> Tuple[str, Optional[int]]:
        """
        Le titre sur la liste est souvent comme: "Hard Day (2014)".
        On sépare proprement.
        """
        raw = (raw or "").strip()
        m = re.match(r"^(.*)\s+\((\d{4})\)\s*$", raw)
        if m:
            return m.group(1).strip(), int(m.group(2))
        return raw, None

    @staticmethod
    def _clean_text(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        s = re.sub(r"\s+", " ", s).strip()
        return s or None

    @staticmethod
    def _find_annotation_in_container(container) -> Optional[str]:
        """
        Repère le bloc:
            "Annotation :" puis le texte juste après.
        Sur ta page, c'est visible en clair dans le HTML rendu serveur. :contentReference[oaicite:2]{index=2}
        """
        if not container:
            return None

        # Cherche un noeud texte qui contient "Annotation"
        ann_node = container.find(string=re.compile(r"^\s*Annotation\s*:\s*$"))
        if not ann_node:
            # parfois le ":" n'est pas exactement au même endroit -> plus tolérant
            ann_node = container.find(string=re.compile(r"\bAnnotation\b", re.IGNORECASE))
            if not ann_node:
                return None

        # Le commentaire est généralement le prochain texte significatif après "Annotation :"
        nxt = ann_node
        for _ in range(20):
            nxt = nxt.find_next(string=True)
            if nxt is None:
                break
            txt = SensCritiqueListScraper._clean_text(str(nxt))
            if not txt:
                continue
            # on ignore les répétitions "Annotation" ou ":" si jamais
            if re.fullmatch(r"Annotation\s*:?", txt, flags=re.IGNORECASE):
                continue
            return txt

        return None

    @staticmethod
    def _find_user_rating(container) -> Optional[str]:
        """
        Repère une mention du style:
            "<pseudo> a mis 6/10."
        (visible sur ta page). :contentReference[oaicite:3]{index=3}
        """
        if not container:
            return None
        text = container.get_text(" ", strip=True)
        m = re.search(r"\ba mis\s+(\d{1,2}/10)\b", text)
        return m.group(1) if m else None

    def parse_page(self, soup: BeautifulSoup, base_url: str) -> List[ListEntry]:
        """
        Extraction d'une page:
        - chaque film a un lien /film/...
        - le titre est dans le texte du lien, souvent "Titre (YYYY)"
        - le commentaire éventuel est dans un bloc "Annotation : ..."
        """
        entries: List[ListEntry] = []

        film_links = soup.select('a[href*="/film/"]')
        seen_film_urls = set()

        for a in film_links:
            href = a.get("href")
            if not href:
                continue

            # Normalise URL
            film_url = urljoin(base_url, href)

            # Déduplique (il peut y avoir d'autres liens vers le film dans le même bloc)
            if film_url in seen_film_urls:
                continue

            raw_title = a.get_text(strip=True) or a.get("title") or a.get("aria-label") or ""
            raw_title = raw_title.strip()
            if not raw_title:
                continue

            # On vise surtout les liens-titres de la liste (souvent "Titre (2014)")
            # => petite validation: contient une année entre parenthèses OU ressemble à un titre non vide
            title, year = self._extract_year_from_title(raw_title)

            # Remonte vers un container "raisonnable" (item de liste)
            container = a
            for _ in range(8):
                if container is None or container.parent is None:
                    break
                container = container.parent
                # heuristique: si le container contient "a mis X/10" ou "Annotation", on est probablement au bon niveau
                txt = container.get_text(" ", strip=True)
                if "a mis" in txt or "Annotation" in txt:
                    break

            user_rating = self._find_user_rating(container)
            annotation = self._find_annotation_in_container(container)

            entries.append(
                ListEntry(
                    title=title,
                    year=year,
                    film_url=film_url,
                    user_rating=user_rating,
                    annotation=annotation,
                )
            )
            seen_film_urls.add(film_url)

        return entries

    def scrape_list(self, list_url: str, max_pages: int = 200) -> List[ListEntry]:
        """
        Parcourt ?page=1..n jusqu'à ce qu'une page ne contienne plus d'entrées.
        (Sur ta liste on voit 1 2 3 4, donc ~4 pages). :contentReference[oaicite:4]{index=4}
        """
        u = urlparse(list_url)
        base_url = f"{u.scheme}://{u.netloc}"

        all_entries: List[ListEntry] = []
        seen: Dict[str, ListEntry] = {}

        for page in range(1, max_pages + 1):
            page_url = self._set_page(list_url, page)
            soup = self._fetch_soup(page_url)

            entries = self.parse_page(soup, base_url=base_url)

            # Condition d'arrêt: plus rien trouvé
            if not entries:
                break

            # Dédup globale
            for e in entries:
                if e.film_url not in seen:
                    seen[e.film_url] = e

            time.sleep(self.sleep_s)

        all_entries = list(seen.values())
        return all_entries


def export_jsonl(entries: List[ListEntry], path: str) -> None:
    import json
    with open(path, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(asdict(e), ensure_ascii=False) + "\n")


def export_csv(entries: List[ListEntry], path: str) -> None:
    import csv
    fields = ["title", "year", "film_url", "user_rating", "annotation"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for e in entries:
            w.writerow(asdict(e))


# if __name__ == "__main__":
#     url = "https://www.senscritique.com/liste/vus_en_2015/781675?page=1"
#     scraper = SensCritiqueListScraper(sleep_s=1.2)

#     entries = scraper.scrape_list(url)

#     print(f"{len(entries)} films récupérés")
#     for e in entries[:10]:
#         print("-", e.title, e.year, e.user_rating, "|", (e.annotation or "")[:80])

#     export_csv(entries, "senscritique_vus_en_2015.csv")
#     export_jsonl(entries, "senscritique_vus_en_2015.jsonl")
#     print("Export: senscritique_vus_en_2015.csv / senscritique_vus_en_2015.jsonl")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape une liste SensCritique (films + annotations)"
    )
    parser.add_argument(
        "--url",
        required=True,
        help="URL de la liste SensCritique (page=1 ou sans page)"
    )
    parser.add_argument(
        "--out",
        default="senscritique_list.csv",
        help="Fichier CSV de sortie"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.2,
        help="Pause entre pages (secondes)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=200,
        help="Nombre max de pages à parcourir"
    )

    args = parser.parse_args()

    scraper = SensCritiqueListScraper(
        sleep_s=args.sleep
    )

    entries = scraper.scrape_list(
        list_url=args.url,
        max_pages=args.max_pages
    )

    print(f"{len(entries)} films récupérés")
    for e in entries[:10]:
        print("-", e.title, e.year, e.user_rating, "|", (e.annotation or "")[:80])

    export_csv(entries, args.out)
    export_jsonl(entries, args.out.replace(".csv", ".jsonl"))
    print(f"Export: {args.out} / {args.out.replace('.csv', '.jsonl')}")