#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import requests
import json
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor


THREADS_NO = 8  # Def:8
BLOCKS_NO = 4

def get_block_url(block_height, format='json'):
    return f'https://blockchain.info/block-height/{block_height}?format={format}'

def get_block_data(block_height, format='json'):
    try:
        block_url = get_block_url(block_height, format)

        for i in range(0, 5):
            try:
                response = requests.get(url=block_url)
                block = response.json()
                break
            except json.JSONDecodeError as e:
                block = None
                if response.text[0] == 'B': # Block not found
                    return None, False

        return block['blocks'][0], True

    except requests.exceptions.RequestException as e:
        logging.exception(e)

def get_tx_url(tx_hash, format='hex'):
    return f'https://blockchain.info/tx/{tx_hash}?format={format}'

def get_tx_data(tx_hash, format='hex'):
    try:
        tx_url = get_tx_url(tx_hash, format)

        for i in range(0, THREADS_NO * 10):
            tx = requests.get(url=tx_url).text

            # Retry if the response is an HTML/plain text error message:
            # - (<)html>[...]429 Too Many Requests[...]
            # - (M)aximum concurrent requests[...]
            # - (T)ransaction not found
            # - (I)nternal Server Error
            # - (An) attempt by a client[...]
            if tx[0] != '<' \
                and tx[0] != 'M' \
                and tx[0] != 'T' \
                and tx[0] != 'I' \
                and (tx[0] != 'A' and tx[1] != 'n'):
                break
            else:
                tx = None
                time.sleep(1)

        return tx
    except requests.exceptions.RequestException:
        logging.exception()

def download_txs(tx_hash):
    logging.info(f'Downloading tx: {tx_hash}')
    tx_hex = get_tx_data(tx_hash)
    if not tx_hex:
        raise RuntimeError()
    return tx_hex

def dump_block(block):
    if not os.path.exists('blocks_H'):
        os.mkdir('blocks_H')
    with open(f"./blocks_H/{block['height']}.json", 'w') as output:
        json.dump(block, output)

def exists_block(height):
    return os.path.isfile(f"./blocks_H/{height}.json")


def download(height=0):
    if len(sys.argv) == 2:
        height = int(sys.argv[1])

    block_height = 0 # Genesis block height
    block_height += height
    temp_index = 0

    running = True
    while(running):
        if exists_block(height):
            block_height += 1
            temp_index += 1
            logging.info(f'Skipped block {height}')
            height += 1
            continue

        if temp_index >= BLOCKS_NO:  # Para descargar tan solo los N primeros
            logging.warn('\nStop block download...')
            running = False
            continue

        logging.info(f'\n********************* NEW BLOCK *********************')
        block_url = get_block_url(block_height, 'text')
        logging.info(f'%s' %block_url)

        logging.info(f'Retrieving block #{block_height}')
        block, found = get_block_data(block_height)

        if found:
            block_height_R = block['height']
            logging.info(f'Retrieved block #{block_height_R}')

        if not block:
            block_height += 1
            logging.warn('Skipping, block index NULL...')
            continue

        height = block['height'] + 1
        block_height += 1
        temp_index += 1

        with ThreadPoolExecutor(max_workers=THREADS_NO) as executor:
            futures = []
            for tx in block['tx']:
                tx_hash = tx['hash']
                future = executor.submit(download_txs, tx_hash)
                futures.append(future)

            try:
                for future, tx in zip(futures, block['tx']):
                    tx['hex'] = future.result()
            except RuntimeError:
                logging.exception('Error retrieving transaction. Skipping block...')
                continue

        dump_block(block)



# Con block height no hay límite en la altura del bloque, hasta el nivel actual
    logging.getLogger().setLevel(logging.INFO)
    if len(sys.argv) == 2:
        initBlock = int(sys.argv[1])
    else:
        initBlock = 190031

    download(initBlock)    # 190031-> 1 Tx
    print('\n')
