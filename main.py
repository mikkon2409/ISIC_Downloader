from isic_api import ISICApi
from config import Config
from threading import Thread
from datetime import datetime
import json
import os
import sys
from logging import info, basicConfig, INFO, debug

basicConfig(level=INFO)


def download_images_info(api: ISICApi) -> list:
    all_images = list()
    images_per_time = 30000
    images_downloaded = 0
    info(f'Fetching of images info started')
    while True:
        images: list = api.getJson(f'image?limit={images_per_time}&offset={images_downloaded}&sort=name')
        images_received = len(images)
        images_downloaded += images_received
        all_images.extend(images)
        info(f'Fetching of images info\tDownloaded: {images_downloaded}')
        if images_received != images_per_time:
            break
    return all_images


def download_images_meta(api: ISICApi,
                         images_info: list,
                         from_idx: int,
                         num: int,
                         output: list,
                         thread_num: int):
    for image in images_info[from_idx:from_idx + num]:
        time = datetime.now()
        image_detail: dict = api.getJson(f'image/{image["_id"]}')
        image_segmentation_data = api.getJson(f'segmentation?imageId={image["_id"]}')
        image_detail.update({'segmentation': image_segmentation_data})
        output.append(image_detail)
        debug(f'Thread#{thread_num}  Fetching of images details: {len(output)} from {num}\t'
              f'({round(len(output) / num * 100, 1)}%)\t'
              f'time remaining: {(datetime.now() - time) * (num - len(output))}')


def download_images(api: ISICApi,
                    images_info: list,
                    from_idx: int,
                    num: int,
                    path: str,
                    thread_num: int):
    downloaded = 0
    for image in images_info[from_idx:from_idx + num]:
        image_file_output_path = os.path.join(path, f'{image["_id"]}.jpg')
        time = datetime.now()
        image_file_resp = api.get(f'image/{image["_id"]}/download')
        image_file_resp.raise_for_status()
        with open(image_file_output_path, 'wb') as imageFileOutputStream:
            for chunk in image_file_resp:
                imageFileOutputStream.write(chunk)
        downloaded += 1

        debug(f'Thread#{thread_num}  Fetching of images details: {downloaded} from {num}\t'
              f'({round(downloaded / num * 100, 1)}%)\t'
              f'time remaining: {(datetime.now() - time) * (num - downloaded)}')


def download_segmentation(api: ISICApi,
                          images_info: list,
                          from_idx: int,
                          num: int,
                          path: str,
                          thread_num: int):
    downloaded = 0
    for image in images_info[from_idx:from_idx + num]:
        time = datetime.now()
        for segmentation in image['segmentation']:
            segmentation_file_output_path = os.path.join(path, f'{segmentation["_id"]}.jpg')
            if os.path.exists(segmentation_file_output_path):
                continue
            image_file_resp = api.get(f'segmentation/{segmentation["_id"]}/mask')
            image_file_resp.raise_for_status()
            with open(segmentation_file_output_path, 'wb') as imageFileOutputStream:
                for chunk in image_file_resp:
                    imageFileOutputStream.write(chunk)
        downloaded += 1

        debug(f'Thread#{thread_num}  Fetching of images details: {downloaded} from {num}\t'
              f'({round(downloaded / num * 100, 1)}%)\t'
              f'time remaining: {(datetime.now() - time) * (num - downloaded)}')


def main():
    username = sys.argv[1]
    password = sys.argv[2]

    info(f'Username: {username}\tPassword: {password}')
    api = ISICApi(username=username, password=password)

    if not os.path.exists(Config.WORKSPACE_PATH):
        os.mkdir(Config.WORKSPACE_PATH)

    path_to_images_meta = os.path.join(Config.WORKSPACE_PATH, Config.IMAGES_META)
    if not os.path.exists(path_to_images_meta):
        all_images = download_images_info(api)
        outputs = list()
        for _ in range(Config.NUM_THREADS):
            outputs.append(list())
        threads = list()
        for thread_idx in range(Config.NUM_THREADS):
            from_idx = (thread_idx + 0) * len(all_images) // Config.NUM_THREADS
            to_idx = (thread_idx + 1) * len(all_images) // Config.NUM_THREADS
            num_images = to_idx - from_idx
            thread = Thread(target=download_images_meta,
                            args=(api,
                                  all_images,
                                  from_idx,
                                  num_images,
                                  outputs[thread_idx],
                                  thread_idx))
            thread.setDaemon(True)
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        images_meta = list()
        for output in outputs:
            images_meta.extend(output)
        if len(images_meta) != 0:
            with open(path_to_images_meta, "w") as write_file:
                json.dump(images_meta, write_file, indent=4)
    else:
        with open(path_to_images_meta, "r") as read_file:
            images_meta = json.load(read_file)

    info(f'Number of images before script execution: {len(images_meta)}')

    if False:
        segmentation_path = os.path.join(Config.WORKSPACE_PATH, Config.IMAGES_PATH)
        if not os.path.exists(segmentation_path):
            os.mkdir(segmentation_path)
        images_meta = list()
        for image in images_meta:
            if not os.path.exists(os.path.join(segmentation_path, f'{image["_id"]}.jpg')):
                images_meta.append(image)
        threads = list()
        for thread_idx in range(Config.NUM_THREADS):
            from_idx = (thread_idx + 0) * len(images_meta) // Config.NUM_THREADS
            to_idx = (thread_idx + 1) * len(images_meta) // Config.NUM_THREADS
            num_images = to_idx - from_idx
            thread = Thread(target=download_images,
                            args=(api,
                                  images_meta,
                                  from_idx,
                                  num_images,
                                  segmentation_path,
                                  thread_idx))
            thread.setDaemon(True)
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    if False:
        segmentation_path = os.path.join(Config.WORKSPACE_PATH, Config.SEGMENTATION_PATH)
        if not os.path.exists(segmentation_path):
            os.mkdir(segmentation_path)
        threads = list()
        for thread_idx in range(Config.NUM_THREADS):
            from_idx = (thread_idx + 0) * len(images_meta) // Config.NUM_THREADS
            to_idx = (thread_idx + 1) * len(images_meta) // Config.NUM_THREADS
            num_images = to_idx - from_idx
            thread = Thread(target=download_segmentation,
                            args=(api,
                                  images_meta,
                                  from_idx,
                                  num_images,
                                  segmentation_path,
                                  thread_idx))
            thread.setDaemon(True)
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    if False:
        unfinded_masks = [
            "584727129fc3c10f04859aad",
            "58470b479fc3c10f04859672"
        ]
        images_meta = object()
        with open(path_to_images_meta, "r") as read_file:
            images_meta = json.load(read_file)
        for image in images_meta:
            for segmentation in image["segmentation"]:
                if segmentation["_id"] in unfinded_masks:
                    info(f'Segmentation {segmentation["_id"]} will removed')
                    image["segmentation"].remove(segmentation)
        if len(images_meta) != 0:
            with open(path_to_images_meta, "w") as write_file:
                json.dump(images_meta, write_file, indent=4)
                info("File written")

    if False:
        images_meta = object()
        with open(path_to_images_meta, "r") as read_file:
            images_meta = json.load(read_file)
        for image in images_meta:
            if len(image["segmentation"]) == 0:
                info(f'Image {image["_id"]} will removed')
                images_meta.remove(image)
        if len(images_meta) != 0:
            with open(path_to_images_meta, "w") as write_file:
                json.dump(images_meta, write_file, indent=4)
                info("File written")

    if False:
        with open(path_to_images_meta, "r") as read_file:
            images_meta = json.load(read_file)
            images_ids = []
            for image in images_meta:
                images_ids.append(image["_id"] + ".jpg")
            images_path = os.path.join(Config.WORKSPACE_PATH, Config.IMAGES_PATH)
            for image in os.listdir(images_path):
                if image not in images_ids:
                    os.remove(os.path.join(images_path, image))
                    info(f'{image} deleted')

    if True:
        with open(path_to_images_meta, "r") as read_file:
            images_meta = json.load(read_file)
            num = 0
            for image in images_meta:
                if "diagnosis" in image["meta"]["clinical"].keys() or \
                        "benign_malignant" in image["meta"]["clinical"].keys():
                    num += 1
            print("Num:", num)


    with open(path_to_images_meta, "r") as read_file:
        images_meta = json.load(read_file)
        info(f'Number of images after script execution: {len(images_meta)}')
        print("Benign:", len(list(filter(lambda x: x["meta"]["clinical"]["benign_malignant"] == "benign", images_meta))))
        print("Malignant:", len(list(filter(lambda x: x["meta"]["clinical"]["benign_malignant"] == "malignant", images_meta))))


if __name__ == "__main__":
    main()
