
import asyncio
# import asyncpg
# import os
from asyncio import Semaphore
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response


async def get(semaphore: Semaphore, url: str, client: AsyncClient):
    async with semaphore:
        return await client.get(url)


def extract_links(r: Response, site: str) -> list[str]:
    soup = BeautifulSoup(r, 'html.parser')
    return [site+e.attrs['href'] for e in soup.select('a.catalog__images--for-desk')]


async def main() -> None:
    base_url = 'https://autoexpert.moscow/cars?page='
    page_count = 323

    semaphore = Semaphore(10)
    client = AsyncClient()

    tasks = [get(semaphore, base_url+str(i), client) for i in range(1, page_count+1)]
    response = await asyncio.gather(*tasks)

    site = 'https://autoexpert.moscow'
    car_urls = []
    for r in response:
        car_urls.extend(extract_links(r, site))

    with open('car_links.txt', 'w') as file:
        file.write('\n'.join(car_urls))


if __name__ == '__main__':
    asyncio.run(main())
