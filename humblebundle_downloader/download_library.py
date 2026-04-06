import os
import sys
import json
import time
import queue
import parsel
import logging
import datetime
import threading
import concurrent.futures
import requests
import http.cookiejar
from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    """Routes log output through tqdm.write() to avoid collision with progress bars."""

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
        except Exception:
            self.handleError(record)

logger = logging.getLogger(__name__)


def _clean_name(dirty_str):
    allowed_chars = (" ", "_", ".", "-", "[", "]")
    clean = []
    for c in dirty_str.replace("+", "_").replace(":", " -"):
        if c.isalpha() or c.isdigit() or c in allowed_chars:
            clean.append(c)

    return "".join(clean).strip().rstrip(".")


class DownloadLibrary:
    def __init__(
        self,
        library_path,
        cookie_path=None,
        cookie_auth=None,
        progress_bar=False,
        ext_include=None,
        ext_exclude=None,
        platform_include=None,
        purchase_keys=None,
        trove=False,
        update=False,
        download_timeout=30,
        retry_count=3,
        max_workers=1,
    ):
        self.library_path = library_path
        self.progress_bar = progress_bar
        self.ext_include = (
            [] if ext_include is None else list(map(str.lower, ext_include))
        )
        self.ext_exclude = (
            [] if ext_exclude is None else list(map(str.lower, ext_exclude))
        )

        if platform_include is None or "all" in platform_include:
            # if 'all', then do not need to use this check
            platform_include = []
        self.platform_include = list(map(str.lower, platform_include))

        self.purchase_keys = purchase_keys
        self.trove = trove
        self.update = update
        self.download_timeout = download_timeout if download_timeout > 0 else None
        self.retry_count = retry_count
        self.max_workers = max_workers
        self._cache_lock = threading.Lock()
        self._overall_bar = None
        self._bar_positions = queue.Queue()
        for i in range(max_workers):
            self._bar_positions.put(i)

        self.session = requests.Session()
        if cookie_path:
            try:
                cookie_jar = http.cookiejar.MozillaCookieJar(cookie_path)
                cookie_jar.load()
                self.session.cookies = cookie_jar
            except http.cookiejar.LoadError:
                # Still support the original cookie method
                with open(cookie_path, "r") as f:
                    self.session.headers.update({"cookie": f.read().strip()})
        elif cookie_auth:
            self.session.headers.update(
                {"cookie": "_simpleauth_sess={}".format(cookie_auth)}
            )

    def start(self):
        self.cache_file = os.path.join(self.library_path, ".cache.json")
        self.cache_data = self._load_cache_data(self.cache_file)
        self.purchase_keys = (
            self.purchase_keys if self.purchase_keys else self._get_purchase_keys()
        )

        # Route all log output through tqdm.write() so messages appear
        # cleanly above progress bars
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        root_logger.handlers.clear()
        tqdm_handler = TqdmLoggingHandler()
        tqdm_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(tqdm_handler)

        # Phase 1: Fetch all data and count total items
        if self.trove is True:
            logger.info("Only checking the Humble Trove...")
            trove_products = self._get_trove_products()
            total_items = sum(
                len(product.get("downloads", {}))
                for product in trove_products
            )
        else:
            orders = []
            total_items = 0
            for order_id in self.purchase_keys:
                order = self._fetch_order(order_id)
                if order is None:
                    continue
                bundle_title = _clean_name(order["product"]["human_name"])
                logger.info("Checking bundle: " + str(bundle_title))
                orders.append((order_id, bundle_title, order))
                for product in order["subproducts"]:
                    for download_type in product["downloads"]:
                        total_items += len(
                            download_type.get("download_struct", [])
                        )

        # Phase 2: Download with known total
        bar_pos = self.max_workers if self.progress_bar else 0
        self._overall_bar = tqdm(
            total=total_items,
            unit=" items",
            desc="Overall",
            position=bar_pos,
            leave=True,
            bar_format="{desc}: {n_fmt}/{total_fmt}{unit} [{elapsed}]",
        )

        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                self._executor = executor
                if self.trove is True:
                    for product in trove_products:
                        title = _clean_name(product["human-name"])
                        self._process_trove_product(title, product)
                else:
                    for order_id, bundle_title, order in orders:
                        for product in order["subproducts"]:
                            self._executor.submit(
                                self._process_product,
                                order_id,
                                bundle_title,
                                product,
                            )
                # Wait for all submitted downloads to complete
                executor.shutdown(wait=True)
        finally:
            self._overall_bar.close()
            self._overall_bar = None
            root_logger.removeHandler(tqdm_handler)
            root_logger.handlers = original_handlers

    def _item_done(self):
        if self._overall_bar is not None:
            self._overall_bar.update(1)

    def _adjust_total(self, count):
        if self._overall_bar is not None:
            with self._cache_lock:
                self._overall_bar.total += count
                self._overall_bar.refresh()

    def _get_trove_download_url(self, machine_name, web_name):
        try:
            sign_r = self.session.post(
                "https://www.humblebundle.com/api/v1/user/download/sign",
                data={
                    "machine_name": machine_name,
                    "filename": web_name,
                },
                timeout=self.download_timeout,
            )
        except Exception:
            logger.error(
                "Failed to get download url for trove product {title}".format(
                    title=web_name
                )
            )
            return None

        logger.debug("Signed url response {sign_r}".format(sign_r=sign_r))
        if sign_r.json().get("_errors") == "Unauthorized":
            logger.critical("Your account does not have access to the Trove")
            sys.exit()
        signed_url = sign_r.json()["signed_url"]
        logger.debug("Signed url {signed_url}".format(signed_url=signed_url))
        return signed_url

    def _process_trove_product(self, title, product):
        for platform, download in product["downloads"].items():
            # Sometimes the name has a dir in it
            # Example is "Broken Sword 5 - the Serpent's Curse"
            # Only the windows file has a dir like
            # "revolutionsoftware/BS5_v2.2.1-win32.zip"
            if self._should_download_platform(platform) is False:
                logger.info(
                    "Skipping {platform} for {product_title}".format(
                        platform=platform, product_title=title
                    )
                )
                self._item_done()
                continue

            web_name = download["url"]["web"].split("/")[-1]
            if self._should_download_file_by_ext_and_log(web_name) is False:
                self._item_done()
                continue

            cache_file_key = "trove:{name}".format(name=web_name)
            file_info = {
                "uploaded_at": (
                    download.get("uploaded_at")
                    or download.get("timestamp")
                    or product.get("date_added", "0")
                ),
                "md5": download.get("md5", "UNKNOWN_MD5"),
            }
            with self._cache_lock:
                cache_file_info = self.cache_data.get(cache_file_key, {})

            if cache_file_info != {} and self.update is not True:
                # Do not care about checking for updates at this time
                self._item_done()
                continue

            if file_info["uploaded_at"] != cache_file_info.get(
                "uploaded_at"
            ) and file_info["md5"] != cache_file_info.get("md5"):
                product_folder = os.path.join(self.library_path, "Humble Trove", title)
                # Create directory to save the files to
                try:
                    os.makedirs(product_folder)
                except OSError:
                    pass
                local_filename = os.path.join(
                    product_folder,
                    web_name,
                )

                if "uploaded_at" in cache_file_info:
                    uploaded_at = time.strftime(
                        "%Y-%m-%d", time.localtime(int(cache_file_info["uploaded_at"]))
                    )
                else:
                    uploaded_at = None

                self._executor.submit(
                    self._download_trove_file,
                    download,
                    web_name,
                    cache_file_key,
                    file_info,
                    local_filename,
                    rename_str=uploaded_at,
                )
            else:
                self._item_done()

    def _download_trove_file(
        self, download, web_name, cache_file_key, file_info,
        local_filename, rename_str=None
    ):
        if rename_str:
            self._rename_old_file(local_filename, rename_str)

        for attempt in range(1 + self.retry_count):
            if attempt > 0:
                wait_time = min(2 ** attempt, 30)
                logger.info(
                    "Retry {attempt}/{retry_count} for {local_filename} "
                    "in {wait}s...".format(
                        attempt=attempt,
                        retry_count=self.retry_count,
                        local_filename=local_filename,
                        wait=wait_time,
                    )
                )
                time.sleep(wait_time)

            # Get a fresh signed URL for each attempt (signatures expire)
            signed_url = self._get_trove_download_url(
                download["machine_name"], web_name
            )
            if signed_url is None:
                continue

            try:
                self._download_file(signed_url, local_filename)
            except KeyboardInterrupt:
                self._remove_partial_file(local_filename)
                sys.exit()
            except Exception as e:
                logger.warning(
                    "Trove download attempt {attempt} failed for "
                    "{local_filename}: {error}".format(
                        attempt=attempt + 1,
                        local_filename=local_filename,
                        error=e,
                    )
                )
                continue

            # Success
            self._update_cache_data(cache_file_key, file_info)
            self._item_done()
            return True

        logger.error(
            "Failed to download trove file {local_filename} after "
            "{attempts} attempts".format(
                local_filename=local_filename,
                attempts=1 + self.retry_count,
            )
        )
        self._remove_partial_file(local_filename)
        self._item_done()
        return False

    def _get_trove_products(self):
        trove_products = []
        idx = 0
        trove_base_url = "https://www.humblebundle.com/client/catalog?index={idx}"
        while True:
            logger.debug(
                "Collecting trove product data from api pg:{idx} ...".format(idx=idx)
            )
            trove_page_url = trove_base_url.format(idx=idx)
            try:
                trove_r = self.session.get(
                    trove_page_url, timeout=self.download_timeout
                )
            except Exception:
                logger.error("Failed to get products from Humble Trove")
                return []

            page_content = trove_r.json()

            if len(page_content) == 0:
                break

            trove_products.extend(page_content)
            idx += 1

        return trove_products

    def _fetch_order(self, order_id):
        order_url = "https://www.humblebundle.com/api/v1/order/{order_id}?all_tpkds=true".format(
            order_id=order_id
        )
        try:
            order_r = self.session.get(
                order_url,
                headers={
                    "content-type": "application/json",
                    "content-encoding": "gzip",
                },
                timeout=self.download_timeout,
            )
        except Exception:
            logger.error("Failed to get order key {order_id}".format(order_id=order_id))
            return None

        logger.debug("Order request: {order_r}".format(order_r=order_r))
        return order_r.json()

    def _rename_old_file(self, local_filename, append_str):
        # Check if older file exists, if so rename
        if os.path.isfile(local_filename) is True:
            filename_parts = local_filename.rsplit(".", 1)
            new_name = "{name}_{append_str}.{ext}".format(
                name=filename_parts[0], append_str=append_str, ext=filename_parts[1]
            )
            os.rename(local_filename, new_name)
            logger.info("Renamed older file to {new_name}".format(new_name=new_name))

    def _process_product(self, order_id, bundle_title, product):
        product_title = _clean_name(product["human_name"])
        # Get all types of download for a product
        for download_type in product["downloads"]:
            num_files = len(download_type.get("download_struct", []))
            if self._should_download_platform(download_type["platform"]) is False:
                logger.info(
                    "Skipping {platform} for {product_title}".format(
                        platform=download_type["platform"], product_title=product_title
                    )
                )
                for _ in range(num_files):
                    self._item_done()
                continue

            product_folder = os.path.join(
                self.library_path, bundle_title, product_title
            )
            # Create directory to save the files to
            try:
                os.makedirs(product_folder)
            except OSError:
                pass

            # Download each file type of a product
            for file_type in download_type["download_struct"]:
                try:
                    if "url" in file_type and "web" in file_type["url"]:
                        # downloadable URL
                        url = file_type["url"]["web"]

                        url_filename = url.split("?")[0].split("/")[-1]

                        if (
                            self._should_download_file_by_ext_and_log(url_filename)
                            is False
                        ):
                            self._item_done()
                            continue

                        cache_file_key = order_id + ":" + url_filename
                        try:
                            self._check_cache_and_download(
                                cache_file_key, url, product_folder, url_filename
                            )
                        except FileExistsError:
                            self._item_done()
                            continue
                        except Exception:
                            logger.exception("Failed to download {url}".format(url=url))
                        self._item_done()
                    elif "asm_config" in file_type:
                        # asm.js game playable directly in the browser
                        game_name = file_type["asm_config"]["display_item"]
                        local_folder = os.path.join(product_folder, game_name)
                        # Create directory to save the files to
                        try:
                            os.makedirs(local_folder, exist_ok=True)  # noqa: E701
                        except OSError:
                            pass  # noqa: E701

                        # get the HTML file that presents the game, used in the Humble web interface iframe
                        asmjs_html_filename = game_name + ".html"
                        asmjs_local_html_filename = game_name + ".local.html"
                        cache_file_key = order_id + ":" + asmjs_html_filename
                        # game_name might be "game" or "game_asm" but the path to the file here always uses the "game_asm" version
                        game_asm_name = file_type["asm_manifest"]["asmFile"].split("/")[
                            2
                        ]
                        asmjs_url = (
                            "https://www.humblebundle.com/play/asmjs/"
                            + game_asm_name
                            + "/"
                            + order_id
                        )

                        if (
                            self._should_download_file_by_ext_and_log(
                                asmjs_html_filename
                            )
                            is False
                        ):
                            self._item_done()
                            continue
                        downloaded = False
                        try:
                            downloaded = self._check_cache_and_download(
                                cache_file_key,
                                asmjs_url,
                                local_folder,
                                asmjs_html_filename,
                            )
                        except FileExistsError:
                            self._item_done()
                            # we should download the asm/data files even if the html file was previously downloaded
                        except Exception:
                            logger.exception(
                                "Failed to download {asmjs_url}".format(
                                    asmjs_url=asmjs_url
                                )
                            )
                            self._item_done()
                            continue
                        else:
                            self._item_done()

                        # read from the html file a version of file_type['asm_manifest'] with HMAC/etc auth params on the URLs
                        with open(
                            os.path.join(local_folder, asmjs_html_filename), "r"
                        ) as asmjs_html:
                            asmjs_page = parsel.Selector(text=asmjs_html.read())
                            asm_player_data_text = asmjs_page.css(
                                "#webpack-asm-player-data::text"
                            ).get()  # noqa: E501
                            asm_player_data = json.loads(asm_player_data_text)

                        if downloaded:
                            # create the local playable version of the html file
                            # by replacing remote manifest URLs with the local filename
                            try:
                                with open(
                                    os.path.join(local_folder, asmjs_html_filename), "r"
                                ) as asmjs_html:
                                    with open(
                                        os.path.join(
                                            local_folder, asmjs_local_html_filename
                                        ),
                                        "w",
                                    ) as asmjs_local_html:
                                        for line in asmjs_html:
                                            for (
                                                local_filename,
                                                remote_file,
                                            ) in asm_player_data["asmOptions"][
                                                "manifest"
                                            ].items():
                                                line = line.replace(
                                                    f'"{local_filename}": "{remote_file}"',
                                                    f'"{local_filename}": "{local_filename}"',
                                                )
                                            asmjs_local_html.write(line)
                            except Exception:
                                logger.exception(
                                    "Failed to create local version of {asmjs_html_filename}".format(
                                        asmjs_html_filename=asmjs_html_filename
                                    )
                                )

                        # TODO deduplicate these files? Osmos example has 3 unique files and 2 dupes with different names
                        manifest_items = asm_player_data["asmOptions"]["manifest"]
                        self._adjust_total(len(manifest_items))
                        for local_filename, remote_file in manifest_items.items():
                            cache_file_key = (
                                order_id + ":" + game_name + ":" + local_filename
                            )
                            try:
                                self._check_cache_and_download(
                                    cache_file_key,
                                    remote_file,
                                    local_folder,
                                    local_filename,
                                )
                            except FileExistsError:
                                self._item_done()
                                continue
                            except Exception:
                                logger.exception(
                                    "Failed to download {url}".format(url=url)
                                )
                                self._item_done()
                                continue
                            self._item_done()

                    elif "external_link" in file_type:
                        logger.info(
                            "External url found: {bundle_title}/{product_title} : {url}".format(
                                bundle_title=bundle_title,
                                product_title=product_title,
                                url=file_type["external_link"],
                            )
                        )
                        self._item_done()

                    else:
                        logger.info(
                            "No downloadable url(s) found: {bundle_title}/{product_title}".format(
                                bundle_title=bundle_title, product_title=product_title
                            )
                        )
                        logger.info(file_type)
                        self._item_done()
                        continue
                except Exception:
                    logger.exception(
                        "Failed to download this 'file':\n{file_type}".format(
                            file_type=file_type
                        )
                    )
                    continue

    def _update_cache_data(self, cache_file_key, file_info):
        with self._cache_lock:
            self.cache_data[cache_file_key] = file_info
            # Update cache file with newest data so if the script
            # quits it can keep track of the progress
            with open(self.cache_file, "w") as outfile:
                json.dump(
                    self.cache_data,
                    outfile,
                    sort_keys=True,
                    indent=4,
                )

    def _check_cache_and_download(
        self, cache_file_key, remote_file, local_folder, local_filename
    ):
        with self._cache_lock:
            cache_file_info = self.cache_data.get(cache_file_key, {})

        if cache_file_info != {} and self.update is not True:
            # Do not care about checking for updates at this time
            raise FileExistsError

        try:
            remote_file_r = self.session.get(
                remote_file, stream=True, timeout=self.download_timeout
            )
        except Exception:
            logger.exception(
                "Failed to check {remote_file}".format(remote_file=remote_file)
            )
            return False

        # Check to see if the file still exists
        if remote_file_r.status_code != 200:
            logger.debug(
                "File unavailable {remote_file} status code {status_code}".format(
                    remote_file=remote_file, status_code=remote_file_r.status_code
                )
            )
            remote_file_r.close()
            return False

        logger.debug(
            "Item request: {remote_file_r}, Url: {remote_file}".format(
                remote_file_r=remote_file_r, remote_file=remote_file
            )
        )
        file_info = {}
        if "Last-Modified" in remote_file_r.headers:
            file_info["url_last_modified"] = remote_file_r.headers["Last-Modified"]
            if file_info["url_last_modified"] == cache_file_info.get(
                "url_last_modified"
            ):
                remote_file_r.close()
                return False
        if "url_last_modified" in cache_file_info:
            last_modified = datetime.datetime.strptime(
                cache_file_info["url_last_modified"], "%a, %d %b %Y %H:%M:%S %Z"
            ).strftime("%Y-%m-%d")
        else:
            last_modified = None

        # Close the metadata check response; _download_file will make its own request
        remote_file_r.close()

        local_file = os.path.join(local_folder, local_filename)
        # Create directory to save the file to, which might not exist if there's a subdirectory included
        try:
            os.makedirs(os.path.dirname(local_file), exist_ok=True)  # noqa: E701
        except OSError:
            raise  # noqa: E701

        return self._process_download(
            remote_file,
            cache_file_key,
            file_info,
            local_file,
            rename_str=last_modified,
        )

    def _remove_partial_file(self, local_filename):
        try:
            os.remove(local_filename)
        except OSError:
            pass

    def _process_download(
        self, url, cache_file_key, file_info, local_filename, rename_str=None
    ):
        if rename_str:
            self._rename_old_file(local_filename, rename_str)

        for attempt in range(1 + self.retry_count):
            if attempt > 0:
                wait_time = min(2 ** attempt, 30)
                logger.info(
                    "Retry {attempt}/{retry_count} for {local_filename} "
                    "in {wait}s...".format(
                        attempt=attempt,
                        retry_count=self.retry_count,
                        local_filename=local_filename,
                        wait=wait_time,
                    )
                )
                time.sleep(wait_time)

            try:
                self._download_file(url, local_filename)
            except KeyboardInterrupt:
                self._remove_partial_file(local_filename)
                sys.exit()
            except Exception as e:
                logger.warning(
                    "Download attempt {attempt} failed for "
                    "{local_filename}: {error}".format(
                        attempt=attempt + 1,
                        local_filename=local_filename,
                        error=e,
                    )
                )
                # Keep partial file for resume on next attempt
                continue

            # Success
            if "url_last_modified" not in file_info:
                file_info["url_last_modified"] = datetime.datetime.now().strftime(
                    "%a, %d %b %Y %H:%M:%S %Z"
                )
            self._update_cache_data(cache_file_key, file_info)
            return True

        # All retries exhausted
        logger.error(
            "Failed to download {local_filename} after {attempts} attempts".format(
                local_filename=local_filename,
                attempts=1 + self.retry_count,
            )
        )
        self._remove_partial_file(local_filename)
        return False

    def _download_file(self, url, local_filename):
        logger.info(
            "Downloading: {local_filename}".format(local_filename=local_filename)
        )

        request_headers = {}
        existing_size = 0

        # Check for partial file to resume
        if os.path.isfile(local_filename):
            existing_size = os.path.getsize(local_filename)
            if existing_size > 0:
                request_headers["Range"] = "bytes={}-".format(existing_size)
                logger.info(
                    "Resuming from byte {size}".format(size=existing_size)
                )

        product_r = self.session.get(
            url,
            stream=True,
            timeout=self.download_timeout,
            headers=request_headers,
        )

        if product_r.status_code == 416:
            # Range not satisfiable - file may be complete or changed
            os.remove(local_filename)
            existing_size = 0
            product_r = self.session.get(
                url, stream=True, timeout=self.download_timeout
            )

        if product_r.status_code not in (200, 206):
            product_r.close()
            raise ValueError(
                "HTTP {code} for {url}".format(
                    code=product_r.status_code, url=url
                )
            )

        resumed = product_r.status_code == 206
        if resumed:
            file_mode = "ab"
        else:
            file_mode = "wb"
            existing_size = 0

        bar_position = None
        try:
            total_length = product_r.headers.get("content-length")
            total_length = int(total_length) if total_length else None
            display_name = os.path.basename(local_filename)

            if self.progress_bar:
                bar_position = self._bar_positions.get()
                progress = tqdm(
                    total=(existing_size + total_length) if total_length else None,
                    initial=existing_size,
                    unit="B",
                    unit_scale=True,
                    desc=display_name,
                    position=bar_position,
                    leave=False,
                    miniters=1,
                )

            dl = 0
            with open(local_filename, file_mode) as outfile:
                for data in product_r.iter_content(chunk_size=4096):
                    dl += len(data)
                    outfile.write(data)
                    if self.progress_bar:
                        progress.update(len(data))

            if self.progress_bar:
                progress.close()

            if total_length is not None:
                if dl < total_length:
                    raise ValueError("Download did not complete")
                if dl > total_length:
                    logger.warning("Downloaded more content than expected")
        finally:
            product_r.close()
            if bar_position is not None:
                self._bar_positions.put(bar_position)

    def _load_cache_data(self, cache_file):
        try:
            with open(cache_file, "r") as f:
                cache_data = json.load(f)
        except FileNotFoundError:
            cache_data = {}

        return cache_data

    def _get_purchase_keys(self):
        try:
            library_r = self.session.get(
                "https://www.humblebundle.com/home/library",
                timeout=self.download_timeout,
            )
        except Exception:
            logger.exception("Failed to get list of purchases")
            return []

        logger.debug("Library request: " + str(library_r))
        library_page = parsel.Selector(text=library_r.text)
        user_data = (
            library_page.css("#user-home-json-data").xpath("string()").extract_first()
        )
        if user_data is None:
            raise Exception("Unable to download user-data, cookies missing?")
        orders_json = json.loads(user_data)
        return orders_json["gamekeys"]

    def _should_download_platform(self, platform):
        platform = platform.lower()
        if self.platform_include and platform not in self.platform_include:
            return False
        return True

    def _should_download_file_by_ext_and_log(self, filename):
        if self._should_download_file_by_ext(filename) is False:
            logger.info("Skipping the file {filename}".format(filename=filename))
            return False
        return True

    def _should_download_file_by_ext(self, filename):
        ext = filename.split(".")[-1]
        return self._should_download_ext(ext)

    def _should_download_ext(self, ext):
        ext = ext.lower()
        if self.ext_include != []:
            return ext in self.ext_include
        elif self.ext_exclude != []:
            return ext not in self.ext_exclude
        return True
