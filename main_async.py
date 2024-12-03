import io
import requests
import pandas as pd
import aiohttp
import asyncio
import time
from bs4 import BeautifulSoup
import datetime
from model import SpimexTradingResults
from database import create_models
from database_async import get_async_session


BASE_URL = "https://spimex.com"
RESULTS_URL = "https://spimex.com/markets/oil_products/trades/results/"


def get_pages() -> int:
    response = requests.get(RESULTS_URL)
    soup = BeautifulSoup(response.text, "lxml")
    pages = soup.find("div", {"class": "bx-pagination"}).find_all("a")
    return int(pages[-2].find("span").text)


async def get_report_links_on_page(page: int) -> list[str]:
    print(f"Processing page: {page}")
    reports = []
    url = f"{RESULTS_URL}?page=page-{page}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            soup = BeautifulSoup(await response.text(), "lxml")
            links = soup.find_all("a", {"class": "accordeon-inner__item-title link xls"}, limit=10)
            links = [link['href'] for link in links]
            for link in links:
                try:
                    if int(link.split("oil_xls_")[1][:4]) < 2023:
                        return reports
                    reports.append(link)
                except Exception as e:
                    print(e)
    return reports


async def process_report(report_url: str) -> (list[SpimexTradingResults], datetime.date):
    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL+report_url) as response:
            response.raise_for_status()

            report_date = report_url.split("oil_xls_")[1][:8]
            report_date = datetime.date(int(report_date[:4]), int(report_date[4:6]), int(report_date[6:]))
            print(f"Processing report: {report_date}")

            data = io.BytesIO(await response.read())

    start_marker = "Единица измерения: Метрическая тонна"
    end_marker = "Итого:"
    skiprows = None

    with pd.ExcelFile(data) as xls:
        for sheet_name in xls.sheet_names:
            sheet_data = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            try:
                start_row = sheet_data[sheet_data.eq(start_marker).any(axis=1)].index[0]
                skiprows = start_row + 2
                break
            except IndexError:
                continue

    if skiprows is None:
        raise ValueError("Кривой файл excel")

    report_df = pd.read_excel(data, skiprows=skiprows)

    end_row = report_df[report_df.eq(end_marker).any(axis=1)].index[0]
    report_df = report_df.iloc[:end_row]

    report_df = report_df.drop(report_df.columns[0], axis=1)

    report_df = report_df[report_df.iloc[:, -1] != "-"]

    records = []
    for _, row in report_df.iterrows():
        records.append(SpimexTradingResults(
            exchange_product_id=row.iloc[0],
            exchange_product_name=row.iloc[1],
            oil_id=row.iloc[0][:4],
            delivery_basis_id=row.iloc[0][4:7],
            delivery_basis_name=row.iloc[2],
            delivery_type_id=row.iloc[0][-1],
            volume=row.iloc[3],
            total=row.iloc[4],
            count=row.iloc[-1],
            date=report_date
        ))
    return records, report_date


async def write_to_db(records: list[SpimexTradingResults], report_date: datetime.date):
    print(f"Writing to DB: {report_date}")
    async with get_async_session() as session:
        await session.add_all(records)



async def main():
    """Асинхронный сбор, обработка и запись данных."""
    # Сбор всех ссылок на отчеты
    report_links = []
    number_of_pages = 2

    # Создаем задачи для сбора ссылок с каждой страницы
    async with aiohttp.ClientSession() as session:
        tasks = [get_report_links_on_page(page) for page in range(1, number_of_pages + 1)]
        all_page_links = await asyncio.gather(*tasks)

    # Распаковываем списки ссылок из каждой страницы в общий список
    for links in all_page_links:
        report_links.extend(links)

    # Проверяем, есть ли ссылки
    if not report_links:
        print("No reports found!")
        return

    print(f"Found {len(report_links)} reports to process.")

    # Обработка отчетов и запись в базу
    processing_tasks = []
    for link in report_links:
        # Создаем задачи для обработки каждого отчета
        processing_tasks.append(
            asyncio.create_task(process_and_store_report(link))
        )

    # Выполняем задачи
    await asyncio.gather(*processing_tasks)


async def process_and_store_report(link):
    """Асинхронно обрабатывает отчет и записывает его в базу."""
    try:
        # Обработка отчета
        records, report_date = await process_report(link)
        if records is not None and report_date is not None:
            # Запись в базу
            await write_to_db(records, report_date)
        else:
            print(f"Skipping invalid report: {link}")
    except Exception as e:
        print(f"Error processing report {link}: {e}")


if __name__ == "__main__":
    start_time = time.time()
    create_models()
    asyncio.run(main())
    finish_time = time.time() - start_time
    print(finish_time)
