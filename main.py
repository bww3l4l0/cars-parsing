import asyncio
import httpx
import os
import asyncpg
import traceback
from asyncio import Semaphore
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response
from link_collecting import get

PHOTOS_DIRECTORY = ...
DB_AUTH = ...


def process_car_name(soup: BeautifulSoup) -> dict[str, str]:
    x = {}
    x['Марка'] = soup.select('.crumbs__link.active')[2].text.replace('\n\t\t', '').replace('\n\t', '')
    x['Модель'] = soup.select('.crumbs__link.active')[3].text.replace('\n\t\t', '').replace('\n\t', '')
    return x


def process_conditions(soup: BeautifulSoup) -> dict[str, float]:
    return dict(zip([e.text for e in soup.select('div.car__rating__item-text span.desc.defaultText:not(.bold)')],
                    [float(e.text) for e in soup.select('div.car__rating__item-text span.desc.defaultText.bold')]))


def process_car_info(soup: BeautifulSoup) -> dict[str, str]:
    return dict(zip([e.text.replace('\n\t\t\t\t\t', '').replace('\n\t\t\t\t', '')
                     for e in soup.select('.desc.backgroundText.grey--text')],
                    [e.text.replace('\n\t\t\t\t\t', '').replace('\n\t\t\t\t', '')
                     for e in soup.select('.wrapper__car__tech-content .desc.defaultText.bold')]                    
                    ))


def process_car_complectation(soup: BeautifulSoup) -> dict[str:list[str]]:
    complectation = []
    for e in soup.select('.car__complectation.fd div:not([class]) .car__complectation-group'):
        complectation.append([ee.text for ee in e.select('.desc.backgroundText')])

    return dict(zip([e.text.replace('\n\t\t\t', '').replace('\n\t\t', '')
                     for e in soup.select('.car__complectation.fd div:not([class]) \
                                          .desc.defaultText.bold.pin.car__complectation-item-value')],
                    complectation))


def get_photo_urls(soup: BeautifulSoup):
    return [e.attrs['data-src'] for e in soup.select('.slider-car__link.watermark__wrapper[data-fancybox=""]')]


async def load_photos(urls: list[str], client: AsyncClient, semaphore: Semaphore):
    tasks = [get(semaphore, url, client) for url in urls]
    responses = await asyncio.gather(*tasks)
    # bin_pic = [img.content for img in bin_pic]

    paths = []
    for response, url in zip(responses, urls):
        path = os.path.join(PHOTOS_DIRECTORY, os.path.basename(url))
        with open(path, 'wb') as file:
            file.write(response.content)
        paths.append(path)
    return paths


def process_car_price(soup: BeautifulSoup) -> int:
    return int(soup.select('.car-buy-upper__price-block.u1 .desc.accent.bold')[0].
               text.strip().replace('₽', '').replace('\xa0', ''))


async def process_car_page(r: Response,
                           client: AsyncClient,
                           semaphore: Semaphore
                           ) -> dict[str, any]:

    # url = 'https://autoexpert.moscow/cars/mitsubishi/lancer/101601'

    soup = BeautifulSoup(r, 'html.parser')

    result = {}
    result.update(process_car_name(soup))
    result.update(process_car_info(soup))
    result.update(process_conditions(soup))
    result.update(process_car_complectation(soup))

    urls = get_photo_urls(soup)
    result['photo_paths'] = await load_photos(urls, client, semaphore)

    # mileage = result['Пробег']
    result['Пробег'] = int(result['Пробег'].replace('\xa0', '').replace('км', ''))
    result['Год выпуска'] = int(result['Год выпуска'])

    gen = result['Поколение']
    del result['Поколение']
    result['Поколение'] = gen

    owners = result['Владельцы']
    del result['Владельцы']
    result['Владельцы'] = owners

    result['price'] = process_car_price(soup)

    # здесь дополнительные обработки
    return result


async def insert(data: dict, pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                f'''
                INSERT INTO "Autos_"(mark, model, year, gearbox, miliage, engine, fuel_type, body_type,
                type_of_drive, color, overall_condition, body_condition, interior_condition,
                technical_condition, view, exterior, theft_protection, multimedia, salon,
                comfort, safety, other, photo_paths, gen , owners, price)
                VALUES ($1 ,$2 , $3 ,$4 , $5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12, $13 ,$14 ,$15,
                $16, $17 ,$18,$19 ,$20 , $21, $22, $23, $24, $25, $26);
                ''', *data.values())
        except:
            traceback.print_exc()
            print(data)


async def main():

    semaphore = Semaphore(10)
    client = AsyncClient(timeout=20)

    with open('car_links.txt', 'r') as file:
        car_urls = file.readlines()
        car_urls = [url.replace('\n', '') for url in car_urls]

    tasks = [get(semaphore, url, client) for url in car_urls]
    responses = await asyncio.gather(*tasks)

    tasks = [process_car_page(r, client, semaphore) for r in responses]
    result = await asyncio.gather(*tasks)
    print(result)

    pool = await asyncpg.create_pool(DB_AUTH)

    tasks = [insert(data, pool) for data in result]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
