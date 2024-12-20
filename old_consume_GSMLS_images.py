from kafka import KafkaConsumer
import asyncio
import aiohttp
import os


# auto_offset_reset is 'earliest' because I want to read from the beginning when program first runs
# This setting doesn't matter for subsequent runs because it will be reading from the committed offset
# enable_auto_commit is 'False' because I'll manually submit the commits after each batch is processed
# This gives more control over reducing duplicate processing in the event of a consumer failure as well as
# better precision of my offset management

async def download_image(image_name: str, image_link: str):

    base_path = 'base path when I buy the external HDD'

    try:
        async with aiohttp.ClientSession() as session:
            # Timeout currently at 180s to give all pages time to load and scrape the data
            async with session.get(image_link, timeout=180) as response:
                # figure out how to get the status code because I know this doesn't work for AIOHttps
                if response.status == 200:

                    with open(os.path.join(base_path, image_name), 'wb') as file:
                        file.write(await response.content.read())

    except TimeoutError as te:
        # Max sure I use the logger for the function
        pass


image_consumer = KafkaConsumer(group_id='Image Consumers',
                               bootstrap_server='localhost:9092',
                               auto_offset_reset='earliest',
                               enable_auto_commit=False,
                               value_deserializer=lambda v: v.decode('utf-8')
                               )

# Using the subscribe method instead of directly assigning a topic so Kafka can
# handle the rebalancing of partitions for me
image_consumer.subscribe(['testImages'])

try:
    while True:
        background_processes = set()
        # Poll for messages
        records_batch = image_consumer.poll(max_records=10, timeout_ms=10000)

        for item in records_batch.values():
            for image_name, image_link in item.item():
                task = asyncio.create_task(download_image(image_name, image_link))
                background_processes.add(task)
                task.add_done_callback(background_processes.discard)

        # await asyncio.gather(*background_processes)
        # Commit this batch once this is done looping
        image_consumer.commit()

except BaseException:
    pass

