import requests
from bs4 import BeautifulSoup
import json
import time
import random
import re
from urllib.parse import urljoin
import schedule
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("parser.log", encoding="utf-8"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger("shop_parser")


BASE_URL = "https://www.zveromir.ru/"
CATEGORIES = [
    "https://www.zveromir.ru/shop/perenoski_dlya_sobak/",
    "https://www.zveromir.ru/shop/suhoy_korm_dlya_koshek/",
    "https://www.zveromir.ru/shop/furminatori/",
]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}
EXECUTION_INTERVAL_DAYS = 3


def makeRequest(url):
    """
    Выполняет запрос к указанному URL и возвращает HTML страницы.
    """
    try:
        # time.sleep(random.uniform(1, 3))
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к {url}: {e}")
        return None


def analyzeCategoryPage(url):
    """
    Анализирует страницу категории для получения названия категории и количества страниц.
    """
    try:
        html = makeRequest(url)
        if not html:
            return url.split("/")[-2].replace("_", " ").title(), 1

        soup = BeautifulSoup(html, "html.parser")

        categoryNameElement = soup.select_one("h1")
        categoryName = categoryNameElement.get_text(separator=" ", strip=True) if categoryNameElement else url.split("/")[-2].replace("_", " ").title()

        pagination = soup.select(".yiiPager .page")
        pagesCount = 1

        if pagination:
            pages = []
            for page in pagination:
                try:
                    pageNum = int(page.get_text(separator=" ", strip=True))
                    pages.append(pageNum)
                except ValueError:
                    continue
            pagesCount = max(pages) if pages else 1

        logger.info(f"Категория: {categoryName}, страниц: {pagesCount}")
        return categoryName, pagesCount

    except Exception as e:
        logger.error(f"Ошибка при анализе страницы категории {url}: {e}")
        return url.split("/")[-2].replace("_", " ").title(), 1


def extractItemsData(html):
    """
    Извлекает данные о товарах из JavaScript переменной в тегах script.
    """
    if not html:
        return {}

    try:
        itemsDataPattern = re.compile(r"var\s+items_v\s*=\s*(\{.*?\});", re.DOTALL)
        match = itemsDataPattern.search(html)

        if match:
            itemsJsonStr = match.group(1)

            try:
                itemsData = json.loads(itemsJsonStr)
                return itemsData
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка при парсинге JSON данных из script тега: {e}")
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных товаров из script тега: {e}")

    return {}


def extractProductId(productElement):
    """
    Извлекает ID товара из HTML-элемента.
    """
    itemIdElement = productElement.select_one("img")
    if itemIdElement and itemIdElement.get("id"):
        return itemIdElement.get("id")[1:]

    return None


def extractProductVariants(itemsData, productId):
    """
    Извлекает варианты товара из данных JavaScript.
    """
    if not productId or productId not in itemsData:
        return []

    variants = []
    for variant in itemsData[productId]:
        variants.append({
            "article": variant.get("art", None),
            "mass": variant.get("mass", None),
            "price": variant.get("price", None)
        })
    return variants


def getProductsInfoFromCatalog(categoryUrl, page=1):
    """
    Получает информацию о товарах из страницы каталога.
    """
    productsInfo = {}

    if page > 1:
        pageUrl = f"{categoryUrl}page/{page}/"
    else:
        pageUrl = categoryUrl

    logger.info(f"Обработка страницы категории: {pageUrl}")
    html = makeRequest(pageUrl)

    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")

        itemsData = extractItemsData(html)

        productElements = soup.select(".goodsBlock .goods")

        for product in productElements:
            productLink = product.select_one("a")
            if not productLink or not productLink.get("href"):
                continue

            fullUrl = urljoin(BASE_URL, productLink.get("href"))

            productId = extractProductId(product)
            if not productId:
                continue

            if productId not in productsInfo:

                productsInfo[productId] = {
                    "url": fullUrl,
                    "product_id": productId,
                    "variants": []
                }

            productsInfo[productId]["variants"] = extractProductVariants(itemsData, productId)

        result = list(productsInfo.values())
        logger.info(f"Найдено {len(result)} товаров на странице {page} категории")
        return result

    except Exception as e:
        logger.error(f"Ошибка при получении информации о товарах со страницы {page} категории {categoryUrl}: {e}")
        return []


def parseProduct(productInfo):
    """
    Парсит страницу товара для получения подробной информации.
    """
    url = productInfo.get("url", "")
    variants = productInfo.get("variants", [])

    try:
        logger.info(f"Парсинг товара: {url}")
        html = makeRequest(url)
        if not html:
            raise ValueError("Не удалось получить HTML страницы товара")

        soup = BeautifulSoup(html, "html.parser")

        nameElement = soup.select_one("h1")
        name = nameElement.get_text(separator=" ", strip=True) if nameElement else None

        descriptionElement = soup.select_one("[itemprop=description]")
        description = descriptionElement.get_text(separator=" ", strip=True).replace("\xa0", " ") if descriptionElement else None

        imageElement = soup.select_one(".eslider-main-img")
        image = urljoin(BASE_URL, imageElement.get("src").strip()) if imageElement else None

        return {
            "url": url,
            "name": name,
            "description": description,
            "image": image,
            "variants": variants
        }
    except Exception as e:
        logger.error(f"Ошибка при парсинге товара {url}: {e}")


def parseShop():
    """
    Основная функция парсинга магазина. Обрабатывает все категории и товары.
    """
    logger.info("Начало работы парсера")

    result = []

    for categoryUrl in CATEGORIES:
        try:
            logger.info(f"Обработка категории: {categoryUrl}")

            categoryName, pagesCount = analyzeCategoryPage(categoryUrl)

            categoryData = {
                "name": categoryName,
                "goods": []
            }

            for page in range(1, pagesCount + 1):
                logger.info(f"Обработка страницы {page} из {pagesCount} для категории {categoryName}")

                productsInfo = getProductsInfoFromCatalog(categoryUrl, page)

                for productInfo in productsInfo:
                    productData = parseProduct(productInfo)
                    if productData:
                        categoryData["goods"].append(productData)

            result.append(categoryData)

        except Exception as e:
            logger.error(f"Ошибка при обработке категории {categoryUrl}: {e}")

    saveToJsonFile(result)
    logger.info(f"Работа парсера завершена. Следующий парсинг начнётся автоматически через {EXECUTION_INTERVAL_DAYS} дней")


def saveToJsonFile(data):
    """
    Сохраняет данные в JSON файл.
    """
    try:
        with open("shop_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("Данные успешно сохранены в файл shop_data.json")
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в JSON файл: {e}")

def runScheduler():
    schedule.every(EXECUTION_INTERVAL_DAYS).days.do(parseShop)

    while True:
        schedule.run_pending()
        time.sleep(3600)

if __name__ == "__main__":
    parseShop()
    runScheduler()
