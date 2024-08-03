import aiohttp
import asyncio
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# Shopify API credentials
SHOPIFY_STORE_URL = 'https://shopname-de.myshopify.com/admin/api/2024-04'
ACCESS_TOKEN = 'shpat_... enter it fully here'

# Inventory location ID
LOCATION_ID = 'the location ID'

# Headers for Shopify API requests
headers = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': ACCESS_TOKEN
}


# Function to fetch all products asynchronously
async def fetch_all_products(session):
    async with session.get(f"{SHOPIFY_STORE_URL}/products.json", headers=headers) as response:
        response.raise_for_status()
        all_products = await response.json()
        return all_products.get('products', [])


# Function to update inventory asynchronously
async def update_inventory(session, sku, new_quantity, all_products, results):
    try:
        # Strip whitespace and ensure SKU is a string
        sku = str(sku).strip()

        # Filter products based on the provided SKU
        filtered_products = [product for product in all_products for variant in product.get('variants', []) if
                             str(variant.get('sku')).strip() == sku]

        # Check if product is found
        if not filtered_products:
            results['failed'].append((sku, "No product found"))
            return

        # Assuming only one product matches the SKU
        product = filtered_products[0]
        variant = [variant for variant in product.get('variants', []) if str(variant.get('sku')).strip() == sku][0]
        inventory_item_id = variant['inventory_item_id']

        # Update inventory quantity
        update_data = {
            'location_id': int(LOCATION_ID),
            'inventory_item_id': int(inventory_item_id),
            'available': int(new_quantity)
        }

        async with session.post(f"{SHOPIFY_STORE_URL}/inventory_levels/set.json", headers=headers,
                                json=update_data) as update_response:
            update_response.raise_for_status()
            results['succeeded'].append(sku)

    except aiohttp.ClientError as http_err:
        results['failed'].append((sku, f"HTTP error: {http_err}"))
    except Exception as err:
        results['failed'].append((sku, f"Error: {err}"))


async def main():
    # Load the CSV file
    inventory_df = pd.read_csv('inventory_update.csv', delimiter=';')

    # Split the combined column into two separate columns
    inventory_df[['Variant SKU', 'Variant Inventory Qty']] = inventory_df[
        'Variant SKU,Variant Inventory Qty'].str.split(',', expand=True)

    # Drop the original combined column
    inventory_df.drop(columns=['Variant SKU,Variant Inventory Qty'], inplace=True)

    # Strip whitespace and ensure SKUs are strings in the DataFrame
    inventory_df['Variant SKU'] = inventory_df['Variant SKU'].astype(str).str.strip()

    results = {'succeeded': [], 'failed': []}

    async with aiohttp.ClientSession() as session:
        # Fetch all products once
        all_products = await fetch_all_products(session)

        # Create tasks for updating inventory
        tasks = []
        for _, row in inventory_df.iterrows():
            sku = row['Variant SKU']
            quantity = row['Variant Inventory Qty']
            tasks.append(update_inventory(session, sku, quantity, all_products, results))

        # Execute tasks concurrently
        await asyncio.gather(*tasks)

    # Print summary of results
    logger.info("\nSUMMARY:")
    logger.info(f"Successfully updated SKUs: {', '.join(results['succeeded']) if results['succeeded'] else 'None'}")

    if results['failed']:
        logger.info("Failed to update SKUs:")
        for sku, reason in results['failed']:
            logger.info(f"SKU: {sku}, Reason: {reason}")


# Create and run the event loop explicitly
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

loop.run_until_complete(main())
