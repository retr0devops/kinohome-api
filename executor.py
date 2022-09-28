import base64
import datetime
from json import dumps, JSONDecodeError
from threading import Thread
from time import sleep

import psycopg2

from psycopg2.extras import RealDictCursor
from requests import get

import secrets
import string

import config

default_poster = 'https://lh3.googleusercontent.com/' \
                 'proxy/DzV5Cq2ESyjEMvlpcsPVlHULtn4' \
                 'Zhjh1SzNSSkgxY0F5Tk7WIEYugGkXh1tD' \
                 'ckMG5HzJ-_rIPNYYPRTw4YRpDl7-Tv8'


def get_session():
    conn = psycopg2.connect(dbname=config.Database.name, user=config.Database.user,
                            password=config.Database.password, host=config.Database.host,
                            port="5432", connect_timeout=15,
                            keepalives=1, keepalives_idle=30,
                            keepalives_interval=10,
                            keepalives_count=5)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cur


def gen_secret(length: int):
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def get_formatted_duration(duration: int, is_serial: bool) -> str:
    timedelta = str(datetime.timedelta(seconds=duration)).split(':')
    output = str()

    try:
        hours = timedelta[-3]
        minutes = timedelta[-2]

        if int(hours) != 0:
            output += str(hours) + 'ч. '

        output += minutes + 'м'
    except ValueError:
        output = '0ч'

    if is_serial:
        output += ' / Серия'
    else:
        output += '.'

    return output


def gen_token(feedback: str, role: str,
              cpm: float = config.Settings.default_cpm,
              balance: int = config.Settings.default_balance):
    token = gen_secret(32)
    provider_token = gen_secret(16)

    conn, cur = get_session()

    cur.execute("""
    INSERT INTO 
        tokens (token, provider_token, role, usages, views, cpm, balance, feedback, last_view, last_usage) 
        VALUES (%s, %s, %s, 0, 0, %s, %s, %s, NOW(), NOW());""", (token, provider_token, role, cpm, balance, feedback))
    conn.commit()

    conn.close()

    return token


def get_picture_self_url(out_url: str):
    base_url = 'https://' + str(config.Settings.url) + '/picture/'
    return base_url + base64.b64encode(out_url.encode("UTF-8")).decode("UTF-8").replace('/', 's4l')


def get_picture_out_url(b64_str: str):
    return base64.b64decode(b64_str.encode("UTF-8")).decode("UTF-8").replace('s4l', '/')


def load_kp_data(kp_id: int) -> None:
    conn, cur = get_session()

    def write_general():
        for _ in range(3):
            request = get(
                config.API.kinopoisk + 'v2.1/films/' + str(kp_id),
                params={"append_to_response": "RATING"},
                headers={"X-API-KEY": config.Tokens.kinopoisk}, timeout=3
            )

            if request.status_code == 401:
                sleep(1)
                continue
            else:
                kp_data = request.json()
                break
        else:
            return

        try:
            premiere_splitted = kp_data['data']['premiereWorld'].split('-')
            premiere = datetime.datetime(year=int(premiere_splitted[0]),
                                         month=int(premiere_splitted[1]),
                                         day=int(premiere_splitted[2]))
        except (BaseException, Exception):
            premiere = datetime.datetime(year=int(kp_data['data']['year']), month=1, day=1)

        distributors = dumps(kp_data['data']['distributors'].split(', ') if kp_data['data']['distributors'] else [])
        facts = dumps(kp_data['data']['facts'][:30])
        imdb_id = kp_data.get('externalId', dict()).get('imdbId')
        poster_url = kp_data.get('data', dict()).get('posterUrl', default_poster)
        poster_small_url = kp_data.get('data', dict()).get('posterUrlPreview', default_poster)
        poster = get(poster_url, allow_redirects=False).headers['location']
        poster_small = get(poster_small_url, allow_redirects=False).headers['location']

        cur.execute("""
        UPDATE films 
            SET premiere = %s, distributors = %s, facts = %s, imdb_id = %s, poster = %s, poster_small = %s 
            WHERE id = %s;""",
                    (premiere, distributors, facts, imdb_id if imdb_id else False, poster, poster_small, kp_id))
        conn.commit()

    def write_similars():
        for _ in range(5):
            try:
                request = get(
                    config.API.kinopoisk + 'v2.2/films/' + str(kp_id) + '/similars',
                    headers={"X-API-KEY": config.Tokens.kinopoisk}, timeout=3
                )

                if request.status_code == 401:
                    sleep(1)
                    continue
                else:
                    request = get(config.API.bazon + 'search', params={
                        "token": config.Tokens.bazon,
                        "kp": ",".join(
                            [str(b) for b in [a.get('filmId', 0) for a in request.json().get('items', list())]])
                    }, timeout=3).json()
                    similars = dumps([int(a.get('kinopoisk_id', 0)) for a in request.get('results', list())])
                    break
            except (JSONDecodeError, Exception):
                similars = '[]'
                break
        else:
            return

        cur.execute("""
                UPDATE films 
                    SET similars = %s 
                    WHERE id = %s;""",
                    (similars, kp_id))
        conn.commit()

    def write_frames():
        for _ in range(5):
            try:
                request = get(
                    config.API.kinopoisk + 'v2.1/films/' + str(kp_id) + '/frames',
                    headers={"X-API-KEY": config.Tokens.kinopoisk}, timeout=3
                )

                if request.status_code == 401:
                    sleep(1)
                    continue
                else:
                    frames_data = request.json().get('frames', [])
                    break
            except (JSONDecodeError, Exception):
                frames_data = []
                break
        else:
            return

        frames = []

        for frame in frames_data[:30]:
            frames.append({"frame": frame['image'], "frame_small": frame['preview']})

        frames = dumps(frames)

        cur.execute("""
        UPDATE films 
            SET frames = %s 
            WHERE id = %s;""",
                    (frames, kp_id))
        conn.commit()

    def write_trailers():
        for _ in range(5):
            try:
                request = get(
                    config.API.kinopoisk + 'v2.1/films/' + str(kp_id) + '/videos',
                    headers={"X-API-KEY": config.Tokens.kinopoisk}, timeout=3
                )

                if request.status_code == 401:
                    sleep(1)
                    continue
                else:
                    trailers_data = request.json().get('trailers', [])
                    break
            except (JSONDecodeError, Exception):
                trailers_data = []
                break
        else:
            return

        trailers = []

        for trailer in trailers_data[:30]:
            trailers.append(trailer['url'])

        trailers = dumps(trailers)

        cur.execute("""
        UPDATE films 
            SET trailers = %s 
            WHERE id = %s;""",
                    (trailers, kp_id))
        conn.commit()

    try:
        write_general()
    except (BaseException, Exception):
        conn.rollback()

    try:
        write_similars()
    except (BaseException, Exception):
        conn.rollback()

    try:
        write_frames()
    except (BaseException, Exception):
        conn.rollback()

    try:
        write_trailers()
    except (BaseException, Exception):
        conn.rollback()

    conn.close()


def load_bazon_data(kp_ids: [int]) -> None:
    if not kp_ids:
        return

    request = get(config.API.bazon + 'search', params={
        "token": config.Tokens.bazon,
        "kp": ",".join([str(a) for a in kp_ids])
    }, timeout=3).json()

    conn, cur = get_session()

    films_dicted = dict()

    for film in request.get('results', list()):
        if str(film.get('kinopoisk_id', 0)) in films_dicted.keys():
            films_dicted[str(film.get('kinopoisk_id', 0))]['translation'].append(film['translation'])
        else:
            film['info']['director'] = film['info']['director'].replace(', ', ',')
            film['info']['genre'] = film['info']['genre'].replace(', ', ',').title()
            film['info']['actors'] = film['info']['actors'].replace(', ', ',')
            film['info']['country'] = film['info']['country'].replace(', ', ',')
            film['translation'] = [film['translation']]
            films_dicted[str(film.get('kinopoisk_id', 0))] = film

    for key in films_dicted.keys():
        kp_id = int(films_dicted[key].get('kinopoisk_id', 0))
        name = films_dicted[key].get('info', dict()).get('orig', 'Unknown')
        ru_name = films_dicted[key].get('info', dict()).get('rus', 'Неизвестный')
        poster = films_dicted[key].get('info', dict()).get('poster', default_poster)
        kp_rating = round(float(films_dicted[key].get('info', dict()).get('rating', dict()).get('rating_kp', 0)), 1)
        kp_votes = int(films_dicted[key].get('info', dict()).get('rating', dict()).get('vote_num_kp', 0))
        imdb_rating = round(float(films_dicted[key].get('info', dict()).get('rating', dict()).get('rating_imdb', 0)), 1)
        imdb_votes = int(films_dicted[key].get('info', dict()).get('rating', dict()).get('vote_num_imdb', 0))
        slogan = (films_dicted[key].get('info', dict()).get('slogan', 'false') if films_dicted[key].get(
            'info', dict()).get('slogan', 'false') else 'false').replace(
            '<br>', '\n\n').replace('<', '').replace('>', '')
        description = films_dicted[key].get('info', dict()).get(
            'description', 'false').replace('<br>', '\n\n').replace('<', '').replace('>', '')
        age_rating = int(films_dicted[key].get('info', dict()).get('age', 0))
        duration = int(films_dicted[key].get('info', dict()).get('time', 0))
        countries = dumps(films_dicted[key].get('info', dict()).get('country', 'Неизвестная страна').split(','))
        genres = dumps(films_dicted[key].get('info', dict()).get('genre', 'Неизвестный жанр').split(','))
        actors = dumps(films_dicted[key].get('info', dict()).get('actors', 'Неизвестный актер').split(','))
        directors = dumps(films_dicted[key].get('info', dict()).get('director', 'Неизвестный режиссер').split(','))
        is_serial = films_dicted[key].get('serial', '0') == '1'
        is_camrip = films_dicted[key].get('camrip', '0') == '1'
        is_completed = films_dicted[key].get('end', '0') == '1'
        last_season = int(films_dicted[key].get('last_season', 0))
        last_episode = int(films_dicted[key].get('last_episode', 0))
        premiere = datetime.datetime(year=int(films_dicted[key].get('info', dict()).get('year', 2000)), month=1, day=1)
        last_view = datetime.datetime(year=1970, month=1, day=1)
        view_token = gen_secret(16)
        quality = films_dicted[key].get('quality', 'WEB-DLRip')
        resolution = int(films_dicted[key].get('max_qual', '1080'))
        translation = dumps(films_dicted[key].get('translation', ['Дубляж']))

        try:
            cur.execute("""
            INSERT INTO films (id, imdb_id, name, ru_name, poster, 
                poster_small, kp_rating, kp_votes, imdb_rating, 
                imdb_votes, slogan, description, age_rating,
                trailers, frames, duration, facts, countries, 
                genres, similars, is_serial, is_camrip, is_completed,
                last_season, last_episode, actors, directors, 
                premiere, distributors, views, last_view, view_token, 
                quality, resolution, translation) 
            VALUES (%s, 'false', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, '[]', '[]', %s, '[]', %s, %s, '[]', %s, %s, %s, %s, %s, 
                %s, %s, %s, '[]', 0, %s, %s, %s, %s, %s);""",
                        (kp_id, name, ru_name, poster, poster,
                         kp_rating, kp_votes, imdb_rating, imdb_votes,
                         slogan, description, age_rating, duration,
                         countries, genres, is_serial, is_camrip, is_completed,
                         last_season, last_episode, actors, directors, premiere, last_view,
                         view_token, quality, resolution, translation))
            conn.commit()
        except (Exception, psycopg2.DatabaseError):
            conn.rollback()

        Thread(target=load_kp_data, args=(kp_id,), daemon=True).start()

    conn.close()


def get_film(kp_ids: [int]):
    kp_ids = kp_ids[:30]
    conn, cur = get_session()

    cur.execute("""
    SELECT id 
        FROM films 
        WHERE id = ANY(%s);""", (kp_ids,))
    uncached = list(set(kp_ids) - set([a['id'] for a in cur.fetchall()]))

    load_bazon_data(uncached)

    cur.execute("""
        SELECT * 
            FROM films 
            WHERE id = ANY(%s)
            ORDER BY kp_rating DESC LIMIT 30;""", (kp_ids,))
    data = cur.fetchall()

    conn.close()

    return data


def get_random():
    conn, cur = get_session()

    cur.execute("""
            SELECT * 
                FROM films 
                WHERE kp_rating > 7
                ORDER BY RANDOM() LIMIT 30;""")

    data = cur.fetchall()

    conn.close()

    return data


def get_new():
    conn, cur = get_session()

    cur.execute("""
            SELECT * 
                FROM films 
                WHERE kp_rating > 6.5 
                ORDER BY premiere DESC LIMIT 30;""")

    data = cur.fetchall()

    conn.close()

    return data


def get_top():
    conn, cur = get_session()

    cur.execute("""
            SELECT * 
                FROM films 
                WHERE NOT (id = ANY(ARRAY[45319, 178720, 571919, 
                                          674243, 1007472, 1073233,
                                          1392470, 1235081, 762381, 
                                          743965, 1272394, 842259, 
                                          42866, 1008652, 10282, 77413,
                                          462582, 1404244, 790391, 591822,
                                          46483, 477353, 734161, 615680]))
                ORDER BY kp_rating DESC LIMIT 30;""")

    data = cur.fetchall()

    conn.close()

    return data


def get_popular():
    conn, cur = get_session()

    cur.execute("""
            SELECT * 
                FROM films 
                WHERE kp_rating > 6 AND 
                    last_view >= now() - interval '1 week' AND 
                    premiere >= now() - interval '1 year' 
                ORDER BY views DESC LIMIT 30;""")

    data = cur.fetchall()

    conn.close()

    return data


def get_now():
    conn, cur = get_session()

    cur.execute("""
            SELECT * 
                FROM films 
                WHERE kp_rating > 5 
                ORDER BY last_view DESC LIMIT 30;""")

    data = cur.fetchall()
    data.sort(key=lambda x: x['kp_rating'])

    conn.close()

    return [a for a in reversed(data)]


def get_collections():
    conn, cur = get_session()

    cur.execute("""
            SELECT * 
                FROM compilations;""")

    data = cur.fetchall()

    for compilation in data:
        if compilation['films'] == ['top']:
            compilation['films'] = [a['id'] for a in get_top()]
        elif compilation['films'] == ['new']:
            compilation['films'] = [a['id'] for a in get_new()]
        elif compilation['films'] == ['now']:
            compilation['films'] = [a['id'] for a in get_now()]
        elif compilation['films'] == ['random']:
            compilation['films'] = [a['id'] for a in get_random()]

    conn.close()

    return data


def search(query: str):
    if 'kinopoisk.ru/series/' in query or 'kinopoisk.ru/film/' in query:
        try:
            try:
                kp_splitted = query.split('series/')[1]
            except IndexError:
                kp_splitted = query.split('film/')[1]

            kp_id = kp_splitted.split('/')[0]

            if kp_id.isdigit():
                return get_film([int(kp_id)])
        except Exception as e:
            str(e)

    request = get(config.API.bazon + 'search', params={
        "token": config.Tokens.bazon,
        "title": query
    }, timeout=3).json()

    data = get_film([int(a['kinopoisk_id']) for a in request.get('results', [])])

    conn, cur = get_session()

    translation_dicted = dict()

    for film in request.get('results', []):
        kp_id = int(film.get('kinopoisk_id', 0))
        kp_rating = round(float(film.get('info', dict()).get('rating', dict()).get('rating_kp', 0)), 1)
        kp_votes = int(film.get('info', dict()).get('rating', dict()).get('vote_num_kp', 0))
        imdb_rating = round(float(film.get('info', dict()).get('rating', dict()).get('rating_imdb', 0)), 1)
        imdb_votes = int(film.get('info', dict()).get('rating', dict()).get('vote_num_imdb', 0))
        is_camrip = film.get('camrip', '0') == '1'
        is_completed = film.get('end', '0') == '1'
        last_season = int(film.get('last_season', 0))
        last_episode = int(film.get('last_episode', 0))
        quality = film.get('quality', 'WEB-DLRip')
        resolution = int(film.get('max_qual', '1080'))

        if str(kp_id) in translation_dicted.keys():
            translation_dicted[str(kp_id)].append(film.get('translation', 'Дубляж'))
        else:
            translation_dicted[str(kp_id)] = [film.get('translation', 'Дубляж')]

        cur.execute("""
        UPDATE films 
            SET kp_rating = %s, kp_votes = %s, imdb_rating = %s, imdb_votes = %s,
                is_camrip = %s, is_completed = %s, last_season = %s, last_episode = %s, 
                quality = %s, resolution = %s
            WHERE id = %s;""", (kp_rating, kp_votes, imdb_rating, imdb_votes,
                                is_camrip, is_completed, last_season, last_episode,
                                quality, resolution, kp_id))
        conn.commit()

    for film_id in translation_dicted:

        cur.execute("""
                UPDATE films 
                    SET translation = %s
                    WHERE id = %s;""", (dumps(translation_dicted[film_id]), int(film_id)))
        conn.commit()

    conn.close()

    return data


def get_health():
    methods = {get_film: [1421546], search: 'интерстеллар',
               get_top: None, get_new: None, get_now: None,
               get_random: None, get_collections: None}

    output = []

    for method in methods:
        start = datetime.datetime.now().timestamp()

        if methods[method]:
            method(methods[method])
        else:
            method()

        output.append({"method": method.__name__,
                       "ping": str(round((datetime.datetime.now().timestamp() - start) * 1000)) + 'ms'})

    return output


def append_views(kp_id: int, provider_token: str):
    conn, cur = get_session()

    cur.execute("""
    UPDATE films 
        SET views = views + 1, last_view = NOW() 
        WHERE id = %s;
    UPDATE tokens
        SET views = views + 1, last_view = NOW()
        WHERE provider_token = %s;""", (kp_id, provider_token))
    conn.commit()

    conn.close()


def append_usages(token: str):
    conn, cur = get_session()

    cur.execute("""
    UPDATE tokens
        SET usages = usages + 1, last_usage = NOW()
        WHERE token = %s;
    UPDATE tokens
        SET balance = CASE 
            WHEN cpm <= 0 AND balance > 1 THEN balance
            WHEN cpm <= 0 AND balance < 1 THEN 1
            WHEN cpm > 0 AND balance <= 0 THEN 0
            ELSE balance - (cpm / 1000)
        END
        WHERE token = %s;""", (token, token))
    conn.commit()

    conn.close()


def get_token(token: str):
    conn, cur = get_session()

    cur.execute("""
    SELECT *,
        CASE 
            WHEN cpm > 0 AND balance <= 0 THEN 'false'
            ELSE 'true'
        END
        FROM tokens 
        WHERE token = %s;""", (token,))

    data = cur.fetchone()

    conn.close()

    if not data:
        return {"success": False, "code": 401,
                "message": 'The token is invalid.',
                "token": token if token else '', "provider_token": "", "role": '',
                "usages": 0, "views": 0, "cpm": config.Settings.default_cpm,
                "balance": '0.0 RUB', "feedback": '', "results": []}

    elif data['case'] == 'true':
        return {"success": True, "code": 200,
                "message": 'Success.',
                "token": token, "provider_token": data['provider_token'], "role": data['role'],
                "usages": data['usages'], "views": data['views'], "cpm": data['cpm'],
                "balance": round(data['balance'], 2),
                "feedback": data['feedback'], "results": []}

    elif data['case'] == 'false':
        return {"success": False, "code": 403,
                "message": 'You need to top up your balance. '
                           'To do this, contact https://t.me/{}.'.format(config.Settings.support),
                "token": token, "provider_token": data['provider_token'], "role": data['role'],
                "usages": data['usages'], "views": data['views'], "cpm": data['cpm'],
                "balance": round(data['balance'], 2),
                "feedback": data['feedback'], "results": []}

    else:
        return {"success": False, "code": 403,
                "message": 'An error occurred while validating the token. '
                           'You can contact https://t.me/{}.'.format(config.Settings.support),
                "token": token, "provider_token": data['provider_token'], "role": data['role'],
                "usages": data['usages'], "views": data['views'], "cpm": data['cpm'],
                "balance": round(data['balance'], 2),
                "feedback": data['feedback'], "results": []}


def check_provider_token(provider_token: str):
    conn, cur = get_session()

    cur.execute("""
    SELECT 
        EXISTS(
            SELECT 1 
                FROM tokens 
                    WHERE provider_token = %s);""", (provider_token,))
    data = cur.fetchone()
    conn.close()
    return data['exists']


def prepare_film_output(output: list, provider_token: str):
    for film in output:
        for key in film.keys():
            if film[key] == 'true':
                film[key] = True
            elif film[key] == 'false':
                film[key] = False

        film['platforms'] = {
            "kinopoisk": 'https://www.kinopoisk.ru/{}/{}/'.format(
                "series" if film['is_serial'] else 'film', film['id']),
            "imdb": 'https://www.imdb.com/title/{}/'.format(
                film['imdb_id']) if film['imdb_id'] else False}

        film['ratings'] = {
            "kinopoisk": {"rating": film['kp_rating'], "votes": film['kp_votes']},
            "imdb": {"rating": film['imdb_rating'], "votes": film['imdb_votes']}}

        del film['kp_rating']
        del film['imdb_rating']
        del film['kp_votes']
        del film['imdb_votes']

        film['poster'] = get_picture_self_url(film['poster'])
        film['poster_small'] = get_picture_self_url(film['poster_small'])

        film['frames'] = [{"frame": get_picture_self_url(frame['frame']),
                           "frame_small": get_picture_self_url(frame['frame_small'])} for frame in film['frames']]

        film['premiere'] = film['premiere'].timestamp()
        film['last_view'] = film['last_view'].timestamp()

        base_url = 'https://' + str(config.Settings.url)

        film['player'] = '{}/player/{}/{}/{}'.format(base_url, film['id'], film['view_token'], provider_token)
        film['iframe_player'] = '{}/iframe_player/{}/{}/{}'.format(
            base_url, film['id'], film['view_token'], provider_token)

    return output
