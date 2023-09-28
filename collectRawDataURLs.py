import aiohttp
import asyncio
import pandas as pd
import ijson
from playwright.async_api import async_playwright

SITE_TO_VISIT = 'https://transparency-in-coverage.uhc.com/'

async def fetch_json_links_playwright(url):
    ''' Returns a list of links to JSON files that contain links to the price transparency data we want ''' 
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        async with browser:
            page = await browser.new_page()
            await page.goto(url)
            await asyncio.sleep(5)
            json_links = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('a[href$=".json"]')).map(a => a.href);
            }''')
    return json_links

def extract_matching_urls(json_content):
    ''' Combs through each of the JSON files with links to the actual data and returns URLs to the actual gzipped data files.''' 
    urls = [value for prefix, event, value in ijson.parse(json_content) if event == "string" and value.startswith("http") and value.endswith("z")]
    return urls

async def fetch_gz_links_from_json(session, url):
    ''' Retrieves JSON files that contain links to the price transparency data we want and then returns URLs to the actual data we want.''' 
    try:
        response = await asyncio.wait_for(session.get(url), timeout=60)  # Set a 60-second timeout
        async with response:
            if response.status != 200:
                print(f"Failed to get {url}, status code: {response.status}")
                return []
            text_content = await response.text()
            return extract_matching_urls(text_content)
    except asyncio.TimeoutError:
        print(f"Request to {url} timed out")
        return []

async def main():
    json_links = await fetch_json_links_playwright(SITE_TO_VISIT)
    if not json_links: return

    gz_df = pd.DataFrame({'gz_url': []})
    async with aiohttp.ClientSession() as session:
        for json_url in json_links:
            gz_links = await fetch_gz_links_from_json(session, json_url)
            temp_df = pd.DataFrame({'gz_url': gz_links})
            gz_df = pd.concat([gz_df, temp_df], ignore_index=True)

    gz_df.to_csv('/workspaces/transparent/transparency_json_zipped_links.csv', index=False)

    gz_df.head(1)

if __name__ == "__main__":
    asyncio.run(main())
