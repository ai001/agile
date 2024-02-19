# First import logging
from logger import logger

logger.warning("Starting...")

logger.warning("Setting up signal handler")

import signal
import os, sys
from io import StringIO

def cleanup():
    global get_new_agile_rate_job
    logger.warning("Cleaning up before exiting...")
    logger.info("Removing scheduled jobs")
    get_new_agile_rate_job.remove()

def signal_handler(sig, frame):
    logger.critical('Received Ctrl+C, quitting...')
    cleanup()
    sys.exit(0)

logger.info("Loading Modules...")
from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.schedulers.background import BackgroundScheduler
import telegram

# Import tool modules
import json
from datetime import datetime
from pprint import pprint
from time import sleep
import numpy
import pandas as pd
import prettytable as pt
from tabulate import tabulate
from OctopusAgile import Agile

# Import configs
logger.info("Importing configs")
from config import AGILE_RATE_FETCH_TIME
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS

def get_new_agile_rates(agile):
    logger.info("Fetch Octopus agile rates.")
    now_string = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info("Fetch from".format(now_string))

    rates = agile.get_raw_rates(now_string)

    rates_df = pd.json_normalize(rates)
    rates_df.drop('valid_to', axis=1, inplace=True)
    rates_df.drop('value_exc_vat', axis=1, inplace=True)
    rates_df.drop('payment_method', axis=1, inplace=True)
    rates_df.rename(columns={'valid_from': 'Time', 'value_inc_vat': 'Rate'}, inplace=True)
    rates_df['Time'] = pd.to_datetime(rates_df['Time'], errors='coerce')
    # rates_df['valid_to'] = pd.to_datetime(rates_df['valid_to'], errors='coerce')
    rates_df = rates_df.sort_values('Time')

    
    lowest_4_hours = rates_df.nsmallest(8, 'Rate').sort_values('Time')
    lowest_5_hours = rates_df.nsmallest(10, 'Rate').sort_values('Time')
    lowest_6_hours = rates_df.nsmallest(12, 'Rate').sort_values('Time')

    table = pt.PrettyTable(['SNo', 'Rate', 'Time'])
    table.align['SNo'] = 'l'
    table.align['Rate'] = 'r'
    table.align['Time'] = 'r'
    
    for index, row in rates_df.iterrows():
        table.add_row([f'{index:2d}', f'{row["Rate"]:.3f}', row['Time']])

    logger.info(f'Octopus Agile Next Rates, Averate Rate: {rates_df.loc[:, "Rate"].mean():2.2f}p/kWh\n{table}')

    try:
        for chat_id in TELEGRAM_CHAT_IDS:
            bot.send_message(chat_id=chat_id, text=f'Octopus Agile Next Rates, Averate Rate: {rates_df.loc[:, "Rate"].mean():2.2f}p/kWh<pre>{table}</pre>', parse_mode=telegram.constants.PARSEMODE_HTML)
    except Exception as ec:
            logger.error("Failed in sending message due to error, error message is \"{}\"".format(str(ec)))


    
    table.clear_rows()
    for index, row in lowest_4_hours.iterrows():
        table.add_row([f'{index:2d}', f'{row["Rate"]:.3f}', row['Time']])
    logger.info(f'Octopus Agile Next Rates, Averate Rate: {lowest_4_hours.loc[:, "Rate"].mean():2.2f}p/kWh\n{table}')
    try:
        for chat_id in TELEGRAM_CHAT_IDS:
            bot.send_message(chat_id=chat_id, text=f'Octopus Agile Cheapest 4 Hour Rates, Averate Rate: {lowest_4_hours.loc[:, "Rate"].mean():2.2f}p/kWh<pre>{table}</pre>', parse_mode=telegram.constants.PARSEMODE_HTML)
    except Exception as ec:
            logger.error("Failed in sending message due to error, error message is \"{}\"".format(str(ec)))

    
    table.clear_rows()
    for index, row in lowest_5_hours.iterrows():
        table.add_row([f'{index:2d}', f'{row["Rate"]:.3f}', row['Time']])
    logger.info(f'Octopus Agile Next Rates, Averate Rate: {lowest_5_hours.loc[:, "Rate"].mean():2.2f}p/kWh\n{table}')
    try:
        for chat_id in TELEGRAM_CHAT_IDS:
            bot.send_message(chat_id=chat_id, text=f'Octopus Agile Cheapest 5 Hour Rates, Averate Rate: {lowest_5_hours.loc[:, "Rate"].mean():2.2f}p/kWh<pre>{table}</pre>', parse_mode=telegram.constants.PARSEMODE_HTML)
    except Exception as ec:
            logger.error("Failed in sending message due to error, error message is \"{}\"".format(str(ec)))

    
    
    table.clear_rows()
    for index, row in lowest_6_hours.iterrows():
        table.add_row([f'{index:2d}', f'{row["Rate"]:.3f}', row['Time']])
    logger.info(f'Octopus Agile Next Rates, Averate Rate: {lowest_6_hours.loc[:, "Rate"].mean():2.2f}p/kWh\n{table}')
    try:
        for chat_id in TELEGRAM_CHAT_IDS:
            bot.send_message(chat_id=chat_id, text=f'Octopus Agile Cheapest 6 Hour Rates, Averate Rate: {lowest_6_hours.loc[:, "Rate"].mean():2.2f}p/kWh<pre>{table}</pre>', parse_mode=telegram.constants.PARSEMODE_HTML)
    except Exception as ec:
            logger.error("Failed in sending message due to error, error message is \"{}\"".format(str(ec)))



if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    # Initilize the messaging bot
    bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
    

    agile = Agile('A') # A is the region code for the East England
    
    

    logger.info("Creating Scheduler Object...")
    scheduler = BlockingScheduler()
    
    get_new_agile_rate_job = scheduler.add_job(get_new_agile_rates, 'cron', [agile,], hour=AGILE_RATE_FETCH_TIME.split(':')[0], minute=AGILE_RATE_FETCH_TIME.split(':')[1], id='get_new_agile_rate_job')

    logger.info("Waiting for the scheduled execution...")
    scheduler.start()

    