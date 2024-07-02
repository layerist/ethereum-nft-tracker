import requests
import json
import signal
import sys

# Your Etherscan API key
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    print('Stopping the script...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_latest_block():
    url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}'
    response = requests.get(url)
    data = response.json()
    return int(data['result'], 16)

def get_block_transactions(block_number):
    url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}'
    response = requests.get(url)
    data = response.json()
    transactions = data['result']['transactions']
    addresses = set()
    for tx in transactions:
        if tx['from'] not in addresses:
            addresses.add(tx['from'])
        if tx['to'] and tx['to'] not in addresses:
            addresses.add(tx['to'])
    return addresses

def get_nfts_for_address(address):
    url = f'{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data['result']

def main():
    latest_block = get_latest_block()
    print(f'Latest Block: {latest_block}')
    
    addresses = get_block_transactions(latest_block)
    print(f'Addresses in Block: {addresses}')
    
    nft_addresses = set()
    for address in addresses:
        nfts = get_nfts_for_address(address)
        for nft in nfts:
            nft_addresses.add(nft['contractAddress'])
    
    print(f'NFT Addresses: {nft_addresses}')
    
    with open('nft_addresses.txt', 'w') as f:
        for nft_address in nft_addresses:
            f.write(f'{nft_address}\n')

if __name__ == '__main__':
    main()
