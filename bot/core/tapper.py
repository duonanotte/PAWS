import asyncio
import os
import json
import aiofiles
import random
import brotli
import aiohttp
import traceback
import cloudscraper

from aiocfscrape import CloudflareScraper
from time import time
from urllib.parse import unquote, quote
from random import randint, choices, uniform
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw import types
from pyrogram.raw.functions.messages import RequestAppWebView
from typing import Tuple

from bot.config import settings
from bot.core.agents import generate_random_user_agent
from bot.utils.logger import logger
from bot.exceptions import InvalidSession
from bot.utils.connection_manager import connection_manager
from .headers import headers

end_point = "https://api.paws.community/v1/"
auth_api = f"{end_point}user/auth"
quest_list = f"{end_point}quests/list"
complete_task = f"{end_point}quests/completed"
claim_task = f"{end_point}quests/claim"

class Tapper:
    def __init__(self, tg_client: Client, proxy: str | None):
        self.tg_client = tg_client
        self.session_name = tg_client.name
        self.first_name = ''
        self.last_name = ''
        self.user_id = ''
        self.auth_token = ""
        self.access_token = None
        self.balance = 0
        self.my_ref = "wBuWS75s"
        self.new_account = False
        self.proxy = proxy

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}
        self.headers = headers.copy()

    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        if not settings.USE_PROXY:
            return True
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    async def get_tg_web_data(self) -> str:
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()

                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            while True:
                try:
                    peer = await self.tg_client.resolve_peer('PAWSOG_bot')
                    break
                except FloodWait as fl:
                    fls = fl.value

                    logger.warning(f"<light-yellow>{self.session_name}</light-yellow> | FloodWait {fl}")
                    logger.info(f"<light-yellow>{self.session_name}</light-yellow> | Sleep {fls}s")

                    await asyncio.sleep(fls + 3)

            self.refer_id = settings.REF_ID

            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                platform='android',
                app=types.InputBotAppShortName(bot_id=peer, short_name="PAWS"),
                write_allowed=True,
                start_param=self.refer_id
            ))

            auth_url = web_view.url

            tg_web_data = unquote(
                string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))
            tg_web_data_parts = tg_web_data.split('&')

            user_data = tg_web_data_parts[0].split('=')[1]
            chat_instance = tg_web_data_parts[1].split('=')[1]
            chat_type = tg_web_data_parts[2].split('=')[1]
            start_param = tg_web_data_parts[3].split('=')[1]
            auth_date = tg_web_data_parts[4].split('=')[1]
            hash_value = tg_web_data_parts[5].split('=')[1]

            user_data_encoded = quote(user_data)
            self.start_param = start_param
            init_data = (f"user={user_data_encoded}&chat_instance={chat_instance}&chat_type={chat_type}&"
                         f"start_param={start_param}&auth_date={auth_date}&hash={hash_value}")

            # print(init_data)
            me = await self.tg_client.get_me()
            self.name = me.first_name
            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return init_data

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error during Authorization: {error}")
            await asyncio.sleep(delay=3)
        finally:
            if self.tg_client.is_connected:
                await self.tg_client.disconnect()
            await asyncio.sleep(randint(5, 10))

    async def login(self, http_client: cloudscraper.CloudScraper):
        try:
            payload = {
                "data": self.auth_token,
                "referralCode": self.my_ref
            }
            login = http_client.post(auth_api, json=payload)
            if login.status_code == 201:
                res = login.json()
                data = res['data']

                self.access_token = res['data'][0]
                logger.success(f"{self.session_name} | Successfully logged in!")
                return data
            else:
                print(login.text)
                logger.warning(f"{self.session_name} | <yellow>Failed to login: {login.status_code}</yellow>")
                return None
        except Exception as e:
            logger.error(f"{self.session_name} | Unknown error while trying to login: {e}")
            return None

    async def get_tasks(self, http_client: cloudscraper.CloudScraper):
        try:
            tasks = http_client.get(quest_list)
            if tasks.status_code == 200:
                res = tasks.json()
                if res.get('success', False):
                    data = res['data']
                    logger.info(f"{self.session_name} | Getting list of tasks... | Found {len(data)} tasks")
                    return data
                else:
                    logger.warning(f"{self.session_name} | Failed to get tasks: API returned success=false")
                    return None
            else:
                logger.warning(f"{self.session_name} | Failed to get tasks: {tasks.status_code}")
                return None
        except Exception as e:
            logger.error(f"{self.session_name} | Error while getting tasks: {e}")
            return None

    async def claim_task(self, task, http_client: cloudscraper.CloudScraper, attempt=10):
        if attempt == 0:
            return False

        try:
            task_id = task['_id']
            task_title = task['title']

            # First mark task as completed
            complete_payload = {
                "questId": task_id
            }

            # Try to complete the task first
            complete_response = http_client.post(complete_task, json=complete_payload)
            if complete_response.status_code != 201:
                logger.warning(
                    f"{self.session_name} | Failed to mark task as completed: {task_title} | Status: {complete_response.status_code}")
                await asyncio.sleep(random.uniform(2, 4))
                return await self.claim_task(task, http_client, attempt - 1)

            # Then try to claim the reward
            claim_payload = {
                "questId": task_id
            }

            # logger.info(f"{self.session_name} | Attempt {11 - attempt} to claim task: {task_title}")
            claim_response = http_client.post(claim_task, json=claim_payload)

            if claim_response.status_code == 201:
                res = claim_response.json()
                if res.get('success', False):
                    reward_amount = task['rewards'][0]['amount'] if task['rewards'] else 0
                    # logger.success(
                    #     f"{self.session_name} | Successfully completed task: {task_title} | Earned {reward_amount} PAWS")
                    return True
                else:
                    logger.info(f"{self.session_name} | Failed to claim reward for task: {task_title}, Retrying...")
                    await asyncio.sleep(random.uniform(3, 5))
                    return await self.claim_task(task, http_client, attempt - 1)
            else:
                logger.warning(
                    f"{self.session_name} | Failed to claim {task_title}: Status {claim_response.status_code}")
                await asyncio.sleep(random.uniform(2, 4))
                return await self.claim_task(task, http_client, attempt - 1)

        except Exception as e:
            logger.error(f"{self.session_name} | Unknown error while claiming task: {e}")
            await asyncio.sleep(random.uniform(1, 3))
            return await self.claim_task(task, http_client, attempt - 1)

    async def process_tasks(self, task_list, session, ref_counts):
        for task in task_list:
            task_title = task['title']
            task_code = task.get('code', '')
            task_type = task.get('type', 'unknown')
            task_reward = task['rewards'][0]['amount'] if task['rewards'] else 0
            task_progress = task.get('progress', {})

            # Skip already claimed tasks
            if task_progress.get('claimed', False):
                # logger.info(f"{self.session_name} | Task '{task_title}' already completed")
                continue

            # Check referral task requirements
            if task_code == "invite" and ref_counts < 10:
                # logger.info(f"{self.session_name} | Task '{task_title}' skipped - insufficient referrals")
                continue

            # Skip disabled tasks
            if task_code in settings.DISABLED_TASKS:
                # logger.info(f"{self.session_name} | Task '{task_title}' skipped - in disabled list")
                continue

            # Handle Telegram tasks
            if task_code == "telegram":
                # logger.info(f"{self.session_name} | Task '{task_title}' requires session mode for channel joining")
                continue

            # Handle Twitter/Social tasks
            if task_code == "twitter" or task_type == "social":
                # logger.info(f"{self.session_name} | Processing social task: {task_title}")

                if task.get('action') == "link" and task.get('data'):
                    # Log the task details
                    # logger.info(
                    #     f"{self.session_name} | Executing task: '{task_title}' | "
                    #     f"Type: {task_type} | "
                    #     f"Reward: {task_reward} PAWS"
                    # )

                    # Add delay to simulate human behavior
                    await asyncio.sleep(random.uniform(5, 10))

                    # Try to claim the task
                    success = await self.claim_task(task, session)

                    if success:
                        logger.success(
                            f"{self.session_name} | Task <cyan>'{task_title}'</cyan> completed successfully | "
                            f"Earned <ly>{task_reward}</ly> PAWS"
                        )
                    else:
                        logger.warning(f"{self.session_name} | Failed to complete task '{task_title}'")

                    continue

            success = await self.claim_task(task, session)

            if success:
                logger.success(
                    f"{self.session_name} | Task <cyan>'{task_title}'</cyan> completed successfully | "
                    f"Earned <ly>{task_reward}</ly> PAWS"
                )
            else:
                logger.warning(f"{self.session_name} | Failed to complete task '{task_title}'")

            # Add random delay between tasks
            await asyncio.sleep(random.uniform(5, 10))

    async def get_leaderboard_position(self, session: cloudscraper.CloudScraper) -> int:
        try:
            response = session.get('https://api.paws.community/v1/user/leaderboard?page=0&limit=100')
            if response.status_code == 200:
                data = response.json()
                if data.get('success', False):
                    user_data = data.get('data', {}).get('userData', {})
                    position = user_data.get('position', 0)
                    return position
                else:
                    logger.warning(f"{self.session_name} | Failed to get leaderboard data: API returned success=false")
            else:
                logger.warning(f"{self.session_name} | Failed to get leaderboard position: {response.status_code}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error while getting leaderboard position: {e}")
        return 0

    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(
                f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        if settings.USE_PROXY:
            if not self.proxy:
                logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                return
            proxy_conn = ProxyConnector().from_url(self.proxy)
        else:
            proxy_conn = None

        access_token_created_time = 0

        http_client = CloudflareScraper(headers=headers, connector=proxy_conn)
        session = cloudscraper.create_scraper()
        connection_manager.add(http_client)

        await self.check_proxy(http_client)

        token_live_time = randint(3500, 3600)
        while True:
            try:
                if http_client.closed:
                    if settings.USE_PROXY:
                        if proxy_conn and not proxy_conn.closed:
                            await proxy_conn.close()

                        if not self.proxy:
                            logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                            return
                        proxy_conn = ProxyConnector().from_url(self.proxy)
                    else:
                        proxy_conn = None

                    http_client = CloudflareScraper(headers=headers, connector=proxy_conn)
                    session = cloudscraper.create_scraper()
                    connection_manager.add(http_client)

                if time() - access_token_created_time >= token_live_time:
                    tg_web_data = await self.get_tg_web_data()
                    self.auth_token = tg_web_data
                    access_token_created_time = time()
                    token_live_time = randint(5000, 7000)

                login = await self.login(session)

                if login:
                    http_client.headers['Authorization'] = f"Bearer {self.access_token}"
                    self.headers['Authorization'] = f"Bearer {self.access_token}"
                    session.headers = self.headers.copy()
                    user = login[1]
                    ref_counts = user['referralData']['referralsCount']

                    try:
                        position = await self.get_leaderboard_position(session)

                        logger.info(
                            f"{self.session_name} | Balance: <green>{user['gameData']['balance']}</green> PAWS | "
                            f"Referrals: <cyan>{ref_counts}</cyan> | "
                            f"Leaderboard Position: <cyan>#{position if position > 0 else 'N/A':,}</cyan>"
                        )

                        await asyncio.sleep(random.randint(1, 3))

                        if settings.AUTO_TASK:
                            task_list = await self.get_tasks(session)
                            if task_list:
                                await self.process_tasks(task_list, session, ref_counts)

                    except Exception as e:
                        logger.error(f"{self.session_name} | Error processing user data: {e}")
                        raise e


            except aiohttp.ClientConnectorError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ServerDisconnectedError as error:
                delay = random.randint(900, 1800)
                logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientResponseError as error:
                delay = random.randint(3600, 7200)
                logger.error(
                   f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientError as error:
                delay = random.randint(3600, 7200)
                logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except asyncio.TimeoutError:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except InvalidSession as error:
                logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                raise error


            except json.JSONDecodeError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            except KeyError as error:
                delay = random.randint(1800, 3600)
                logger.error(
                    f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except Exception as error:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Unexpected error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            finally:
                await http_client.close()
                if settings.USE_PROXY and proxy_conn and not proxy_conn.closed:
                    await proxy_conn.close()
                connection_manager.remove(http_client)

                sleep_time = random.randint(settings.SLEEP_TIME[0], settings.SLEEP_TIME[1])
                hours = int(sleep_time // 3600)
                minutes = (int(sleep_time % 3600)) // 60
                logger.info(
                    f"{self.session_name} | Sleep before wake up <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                await asyncio.sleep(sleep_time)


async def run_tapper(tg_client: Client, proxy: str):
    session_name = tg_client.name
    if not proxy:
        logger.error(f"{session_name} | No proxy found for this session")
        return
    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{session_name} | Invalid Session")
