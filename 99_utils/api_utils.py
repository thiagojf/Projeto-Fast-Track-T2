# Databricks notebook source
import requests
import time

def get_api_data(
    url,
    params=None,
    headers=None,
    max_retries=3,
    retry_delay=2,
    timeout=30
):

    for tentativa in range(max_retries):

        try:

            response = requests.get(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            response.raise_for_status()

            return response.json()

        except Exception as e:

            if tentativa == max_retries - 1:
                raise e

            time.sleep(retry_delay)