import requests
import json
import signal
import sys
import logging
from time import sleep
from retrying import retry

# Конфигурация
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'
SLEEP_INTERVAL = 60  # Интервал между запросами, в секундах
OUTPUT_FILE = 'nft_addresses.txt'

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Обработчик сигналов для корректного завершения работы
def signal_handler(sig, frame):
    logging.info('Stopping the script...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Функция с повторной попыткой при ошибках запроса
@retry(wait_fixed=2000, stop_max_attempt_number=5)
def make_request(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_latest_block():
    try:
        url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}'
        data = make_request(url)
        return int(data['result'], 16)
    except requests.RequestException as e:
        logging.error(f"Error fetching latest block: {e}")
        return None

def get_block_transactions(block_number):
    try:
        url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}'
        data = make_request(url)
        transactions = data['result']['transactions']
        addresses = {tx['from'] for tx in transactions if tx['from']}
        addresses.update({tx['to'] for tx in transactions if tx['to']})
        return addresses
    except requests.RequestException as e:
        logging.error(f"Error fetching transactions for block {block_number}: {e}")
        return set()

def get_nfts_for_address(address):
    try:
        url = f'{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}'
        data = make_request(url)
        return data['result']
    except requests.RequestException as e:
        logging.error(f"Error fetching NFTs for address {address}: {e}")
        return []

def save_nft_addresses(nft_addresses):
    try:
        with open(OUTPUT_FILE, 'w') as f:
            f.write('\n'.join(nft_addresses))
        logging.info(f'NFT addresses written to {OUTPUT_FILE}')
    except IOError as e:
        logging.error(f"Error writing to file: {e}")

def main():
    latest_block = get_latest_block()
    if latest_block is None:
        logging.error("Failed to retrieve the latest block.")
        return
    
    logging.info(f'Latest Block: {latest_block}')
    
    addresses = get_block_transactions(latest_block)
    if not addresses:
        logging.info("No addresses found in the latest block.")
        return
    
    logging.info(f'Addresses in Block: {addresses}')
    
    nft_addresses = {nft['contractAddress'] for address in addresses for nft in get_nfts_for_address(address)}
    
    logging.info(f'NFT Addresses: {nft_addresses}')
    
    save_nft_addresses(nft_addresses)

if __name__ == '__main__':
    while True:
        main()
        sleep(SLEEP_INTERVAL)  # Пауза перед следующей итерацией
