import random
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

NEWS_COLUMNS = [
    "source",
    "published_at",
    "date",
    "time",
    "title",
    "link",
    "ticker",
    "rubric",
]

RUS_MONTHS = {
    "января": "01",
    "февраля": "02",
    "марта": "03",
    "апреля": "04",
    "мая": "05",
    "июня": "06",
    "июля": "07",
    "августа": "08",
    "сентября": "09",
    "октября": "10",
    "ноября": "11",
    "декабря": "12",
}


def build_output_dir() -> Path:
    output_dir = Path("data") / "news"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def build_output_name(
    source: str,
    *,
    date_from: str | None = None,
    date_till: str | None = None,
    pages: int | None = None,
    suffix: str = "news",
) -> str:
    safe_source = source.replace(".", "_").replace("-", "_")
    parts = [safe_source, suffix]

    if date_from and date_till:
        parts.append(f"{date_from}_{date_till}")

    if pages is not None:
        parts.append(f"pages_{pages}")

    return "_".join(parts) + ".csv"


class BaseNewsParser(ABC):
    source: str = "unknown"

    def __init__(self, sleep_range=(0.5, 1.5), timeout=15):
        self.df = pd.DataFrame(columns=NEWS_COLUMNS)
        self.sleep_range = sleep_range
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    @abstractmethod
    def fetch_day(self, day: date) -> list[dict]:
        pass

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        for col in NEWS_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df = df[NEWS_COLUMNS]
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        mask = df["published_at"].notna()
        df.loc[mask, "date"] = df.loc[mask, "published_at"].dt.date.astype(str)
        df.loc[mask, "time"] = df.loc[mask, "published_at"].dt.strftime("%H:%M")

        return df.sort_values("published_at", ascending=False).reset_index(drop=True)

    def parse_range(self, date_from: str, date_till: str) -> pd.DataFrame:
        start = pd.to_datetime(date_from).date()
        end = pd.to_datetime(date_till).date()

        all_items = []
        current = start

        while current <= end:
            try:
                print(f"[{self.source}] Парсинг {current}")
                items = self.fetch_day(current)
                all_items.extend(items)
            except Exception as e:
                print(f"[{self.source}] Ошибка за {current}: {e}")

            current += timedelta(days=1)

        if not all_items:
            self.df = pd.DataFrame(columns=NEWS_COLUMNS)
            return self.df

        self.df = self.normalize(pd.DataFrame(all_items))
        return self.df

    def save_csv(
        self,
        path: str | None = None,
        *,
        date_from: str | None = None,
        date_till: str | None = None,
        pages: int | None = None,
    ):
        output_dir = build_output_dir()

        if path is None:
            file_name = build_output_name(
                self.source,
                date_from=date_from,
                date_till=date_till,
                pages=pages,
            )
            output_path = output_dir / file_name
        else:
            custom_path = Path(path)
            output_path = custom_path if custom_path.is_absolute() else output_dir / custom_path
            output_path.parent.mkdir(parents=True, exist_ok=True)

        self.df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"[{self.source}] saved {len(self.df)} rows to {output_path}")

    def _sleep(self):
        time.sleep(random.uniform(*self.sleep_range))

    def _get_soup(self, url: str, *, method: str = "GET", **kwargs):
        self._sleep()
        response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        if not response.encoding or response.encoding.lower() == "iso-8859-1":
            response.encoding = response.apparent_encoding or "utf-8"
        return BeautifulSoup(response.text, "html.parser")

    def _replace_months(self, value: str) -> str:
        value = str(value).strip().lower()
        for month_name, month_num in RUS_MONTHS.items():
            value = value.replace(month_name, month_num)
        return " ".join(value.split())


class SmartLabParser(BaseNewsParser):
    source = "smart-lab.ru"

    def fetch_day(self, day):
        date_str = day.strftime("%Y-%m-%d")
        results = []

        for page in range(1, 6):
            url = (
                f"https://smart-lab.ru/news/date/{date_str}/"
                if page == 1
                else f"https://smart-lab.ru/news/date/{date_str}/page{page}/"
            )

            soup = self._get_soup(url)
            items = soup.find_all("h3", class_="feed title bluid_48504")

            if not items:
                break

            for item in items:
                link_tag = item.find("a")
                if not link_tag:
                    continue

                results.append(
                    {
                        "source": self.source,
                        "published_at": pd.Timestamp(day),
                        "title": link_tag.get("title", "").strip(),
                        "link": "https://smart-lab.ru" + link_tag.get("href", ""),
                        "ticker": None,
                        "rubric": None,
                    }
                )

        return results


class InvestFundsParser(BaseNewsParser):
    source = "investfunds.ru"

    def __init__(self, sleep_range=(0.5, 1.5), timeout=15, max_pages=1000):
        super().__init__(sleep_range=sleep_range, timeout=timeout)
        self.max_pages = max_pages

    def fetch_day(self, day):
        raise NotImplementedError("Для investfunds используй parse_range()")

    def parse_range(self, date_from: str, date_till: str) -> pd.DataFrame:
        start = pd.to_datetime(date_from).date()
        end = pd.to_datetime(date_till).date()

        results = []
        should_stop = False

        for page in range(1, self.max_pages + 1):
            url = f"https://investfunds.ru/news/?limit=50&page={page}"
            soup = self._get_soup(url, method="POST")
            news_list = soup.select("ul.news_list li")

            if not news_list:
                print(f"[{self.source}] новости не найдены на странице {page}")
                break

            print(f"[{self.source}] страница {page}")

            current_date = ""
            page_has_fresh_rows = False

            for item in news_list:
                classes = item.get("class", [])

                if "date" in classes:
                    current_date = item.get_text(strip=True)
                    continue

                if "item" not in classes:
                    continue

                title_tag = item.select_one("div.lnk a.indent_right_10")
                if not title_tag:
                    continue

                time_tag = item.select_one("span.time")
                source_tag = item.select_one("div.lnk a.source")
                raw_time = time_tag.get_text(strip=True) if time_tag else ""
                dt = self._combine_date_time(current_date, raw_time)

                if dt is None:
                    continue

                news_day = dt.date()
                if news_day < start:
                    should_stop = True
                    continue

                if news_day > end:
                    continue

                page_has_fresh_rows = True

                link = title_tag.get("href", "")
                full_link = "https://investfunds.ru" + link if link.startswith("/") else link

                results.append(
                    {
                        "source": self.source,
                        "published_at": dt,
                        "title": title_tag.get_text(strip=True),
                        "link": full_link,
                        "ticker": None,
                        "rubric": source_tag.get_text(strip=True) if source_tag else None,
                    }
                )

            if should_stop and not page_has_fresh_rows:
                print(f"[{self.source}] дошли до даты раньше {date_from}, останавливаемся")
                break

        if not results:
            self.df = pd.DataFrame(columns=NEWS_COLUMNS)
            return self.df

        self.df = self.normalize(pd.DataFrame(results))
        return self.df

    def parse_pages(self, max_pages=10) -> pd.DataFrame:
        results = []

        for page in range(1, max_pages + 1):
            url = f"https://investfunds.ru/news/?limit=50&page={page}"
            soup = self._get_soup(url, method="POST")
            news_list = soup.select("ul.news_list li")

            if not news_list:
                print(f"[{self.source}] новости не найдены на странице {page}")
                continue

            current_date = ""
            for item in news_list:
                classes = item.get("class", [])

                if "date" in classes:
                    current_date = item.get_text(strip=True)
                    continue

                if "item" not in classes:
                    continue

                title_tag = item.select_one("div.lnk a.indent_right_10")
                if not title_tag:
                    continue

                time_tag = item.select_one("span.time")
                source_tag = item.select_one("div.lnk a.source")
                raw_time = time_tag.get_text(strip=True) if time_tag else ""
                dt = self._combine_date_time(current_date, raw_time)

                link = title_tag.get("href", "")
                full_link = "https://investfunds.ru" + link if link.startswith("/") else link

                results.append(
                    {
                        "source": self.source,
                        "published_at": dt,
                        "title": title_tag.get_text(strip=True),
                        "link": full_link,
                        "ticker": None,
                        "rubric": source_tag.get_text(strip=True) if source_tag else None,
                    }
                )

        if not results:
            self.df = pd.DataFrame(columns=NEWS_COLUMNS)
            return self.df

        self.df = self.normalize(pd.DataFrame(results))
        return self.df

    def _combine_date_time(self, raw_date, raw_time):
        raw_date = str(raw_date).strip()
        if not raw_date:
            return None

        if raw_date.lower() == "сегодня":
            date_part = datetime.today().date()
        else:
            try:
                normalized = self._replace_months(raw_date)
                date_part = datetime.strptime(normalized, "%d %m %Y").date()
            except ValueError:
                return None

        try:
            time_part = datetime.strptime(raw_time, "%H:%M").time() if raw_time else datetime.min.time()
        except ValueError:
            time_part = datetime.min.time()

        return datetime.combine(date_part, time_part)


class KommersantParser(BaseNewsParser):
    source = "kommersant.ru"

    rubrics = {
        "Экономика": 3,
        "Бизнес": 4,
        "Финансы": 40,
        "Потребительский рынок": 41,
    }

    def fetch_day(self, day):
        results = []
        date_str = day.strftime("%Y-%m-%d")

        for rubric_name, rubric_id in self.rubrics.items():
            url = f"https://www.kommersant.ru/archive/rubric/{rubric_id}/day/{date_str}"
            soup = self._get_soup(url)
            articles = soup.select("article.rubric_lenta__item")
            print(f"[{self.source}] {rubric_name}: {len(articles)} новостей")

            for article in articles:
                raw_date = article.get("data-article-date", date_str).strip()
                tag_node = article.select_one("p.rubric_lenta__item_tag")
                raw_time = tag_node.get_text(strip=True).split(", ")[-1] if tag_node else ""
                dt = self._combine(raw_date, raw_time)

                results.append(
                    {
                        "source": self.source,
                        "published_at": dt,
                        "title": article.get("data-article-title", "").strip(),
                        "link": article.get("data-article-url", "").strip(),
                        "ticker": None,
                        "rubric": rubric_name,
                    }
                )

        return results

    def _combine(self, raw_date, raw_time):
        try:
            date_part = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except ValueError:
            return None

        try:
            time_part = datetime.strptime(raw_time, "%H:%M").time() if raw_time else datetime.min.time()
        except ValueError:
            time_part = datetime.min.time()

        return datetime.combine(date_part, time_part)


class InterfaxParser(BaseNewsParser):
    source = "interfax.ru"

    def fetch_day(self, day):
        url = f"https://www.interfax.ru/business/news/{day:%Y/%m/%d}/"
        soup = self._get_soup(url)

        blocks = soup.select("div.an > div[data-id]")
        print(f"[{self.source}] найдено {len(blocks)}")

        results = []

        for block in blocks:
            time_tag = block.find("span")
            title_tag = block.find("h3")
            link_tag = block.find("a")

            raw_time = time_tag.get_text(strip=True) if time_tag else ""
            dt = self._combine(day, raw_time)

            results.append(
                {
                    "source": self.source,
                    "published_at": dt,
                    "title": title_tag.get_text(strip=True) if title_tag else "",
                    "link": f"https://www.interfax.ru{link_tag['href']}" if link_tag else "",
                    "ticker": None,
                    "rubric": "Бизнес",
                }
            )

        return results

    def _combine(self, day, raw_time):
        try:
            time_part = datetime.strptime(raw_time, "%H:%M").time() if raw_time else datetime.min.time()
        except ValueError:
            time_part = datetime.min.time()
        return datetime.combine(day, time_part)


def collect_news(date_from: str, date_till: str, investfunds_pages=10) -> pd.DataFrame:
    parsers = [
        SmartLabParser(),
        InvestFundsParser(max_pages=investfunds_pages),
        KommersantParser(),
        InterfaxParser(),
    ]

    frames = []

    for parser in parsers:
        if isinstance(parser, InvestFundsParser):
            df = parser.parse_range(date_from, date_till)
            parser.save_csv(date_from=date_from, date_till=date_till)
        else:
            df = parser.parse_range(date_from, date_till)
            parser.save_csv(date_from=date_from, date_till=date_till)

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    result = result.drop_duplicates(subset=["title", "link"])
    return result.reset_index(drop=True)


if __name__ == "__main__":
    DATE_FROM = "2025-03-01"
    DATE_TILL = "2025-03-03"
    INVESTFUNDS_PAGES = 10

    df = collect_news(DATE_FROM, DATE_TILL, investfunds_pages=INVESTFUNDS_PAGES)

    output_dir = build_output_dir()
    output_path = output_dir / build_output_name(
        "all",
        date_from=DATE_FROM,
        date_till=DATE_TILL,
    )

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(df.head())
    print(f"Всего: {len(df)}")
    print(f"Файл сохранён в {output_path}")
