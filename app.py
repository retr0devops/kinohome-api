import requests
from flask import Flask, render_template, make_response
from flask_restful import Api, reqparse
from flask_cors import CORS, cross_origin
from flask_compress import Compress

import config
import logging

from executor import get_token, get_health, append_usages, gen_secret, get_film, prepare_film_output, get_new, get_top, \
    search, get_random, append_views, check_provider_token, get_now, get_collections, get_popular, get_picture_out_url, \
    default_poster

app = Flask(__name__, template_folder="templates")
Compress(app)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
# app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['JSON_SORT_KEYS'] = False
api = Api(app)
logger = logging.getLogger()


@app.route('/getHealth', methods=['GET'])
@cross_origin()
def health_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = get_health()

    return output, output['code']


@app.route('/getFilm', methods=['GET'])
@cross_origin()
def film_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    parser.add_argument("id")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(
        get_film([int(a) for a in (params['id']).split(',')]) if params['id'] else [], output['provider_token'])

    return output, output['code']


@app.route('/getNew', methods=['GET'])
@cross_origin()
def new_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(get_new(), output['provider_token'])

    return output, output['code']


@app.route('/getNow', methods=['GET'])
@cross_origin()
def now_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(get_now(), output['provider_token'])

    return output, output['code']


@app.route('/getRandom', methods=['GET'])
@cross_origin()
def random_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(get_random(), output['provider_token'])

    return output, output['code']


@app.route('/getPopular', methods=['GET'])
@cross_origin()
def popular_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(get_popular(), output['provider_token'])

    return output, output['code']


@app.route('/getTop', methods=['GET'])
@cross_origin()
def top_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(get_top(), output['provider_token'])

    return output, output['code']


@app.route('/getCollections', methods=['GET'])
@cross_origin()
def collections_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = get_collections()

    return output, output['code']


@app.route('/search', methods=['GET'])
@cross_origin()
def search_handler():
    parser = reqparse.RequestParser()
    parser.add_argument("api_token")
    parser.add_argument("query")
    params = parser.parse_args()

    output = get_token(params['api_token'])

    if not output['success']:
        return output, output['code']
    else:
        append_usages(params['api_token'])
    output['results'] = prepare_film_output(search(str(params['query'])), output['provider_token'])

    return output, output['code']


@app.route('/iframe_player/<kp_id>/<view_token>/<provider_token>', methods=['GET'])
@cross_origin()
def iframe_player_handler(kp_id: str, view_token: str, provider_token: str):
    try:
        data = get_film([int(kp_id)])
        if check_provider_token(provider_token) and len(data) > 0 and data[0]['view_token'] == view_token:
            append_views(int(kp_id), str(provider_token))
            return render_template('iframe_player.html',
                                   id=kp_id,
                                   name=data[0]['ru_name'],
                                   year=data[0]['premiere'].year), 200, {'Content-Type': 'text/html'}
        else:
            return render_template('404.html', domain=config.Settings.domain,
                                   username=config.Settings.support), 404, {'Content-Type': 'text/html'}
    except Exception as e:
        error_uid = gen_secret(8)
        logger.error('ERROR WITH UID {}'.format(error_uid), exc_info=e)
        return render_template('404.html', domain=config.Settings.domain,
                               username=config.Settings.support), 404, {'Content-Type': 'text/html'}


@app.route('/player/<kp_id>/<view_token>/<provider_token>', methods=['GET'])
@cross_origin()
def player_handler(kp_id: str, view_token: str, provider_token: str):
    try:
        data = get_film([int(kp_id)])
        if check_provider_token(provider_token) and len(data) > 0 and data[0]['view_token'] == view_token:
            append_views(int(kp_id), str(provider_token))
            return render_template('player.html',
                                   id=kp_id,
                                   name=data[0]['ru_name'],
                                   year=data[0]['premiere'].year,
                                   age_rating=data[0]['age_rating']), 200, {'Content-Type': 'text/html'}
        else:
            return render_template('404.html', domain=config.Settings.domain,
                                   username=config.Settings.support), 404, {'Content-Type': 'text/html'}
    except Exception as e:
        error_uid = gen_secret(8)
        logger.error('ERROR WITH UID {}'.format(error_uid), exc_info=e)
        return render_template('404.html', domain=config.Settings.domain,
                               username=config.Settings.support), 404, {'Content-Type': 'text/html'}


@app.route('/picture/<picture>', methods=['GET'])
@cross_origin()
def picture_handler(picture: str):
    try:
        picture_url = get_picture_out_url(str(picture))
        data = requests.get(picture_url)
        response = make_response(data.content)
        response.headers.set('Content-Type', data.headers['Content-Type'])
        return response
    except (Exception, BaseException) as e:
        logger.error('', exc_info=e)
        picture_url = default_poster
        data = requests.get(picture_url)
        response = make_response(data.content)
        response.headers.set('Content-Type', 'image/jpeg')
        return response


@app.route('/bazon.txt', methods=['GET'])
@cross_origin()
def bazon_handler():
    with open('templates/bazon.txt', 'rb') as f:
        return f.read()


@app.route('/', methods=['GET'])
@cross_origin()
def index_handler():
    return render_template('index.html',
                           support=config.Settings.support,
                           docs=config.Settings.docs), 200, {'Content-Type': 'text/html'}


@app.errorhandler(404)
def page_not_found_handler(_):
    return render_template('404.html', domain=config.Settings.domain,
                           username=config.Settings.support), 404, {'Content-Type': 'text/html'}


@app.errorhandler(500)
def exception_handler(_):
    output = get_token('')
    error_uid = gen_secret(8)
    logger.error('ERROR WITH UID {}'.format(error_uid))
    output['code'] = 503
    output['message'] = 'An error occurred while executing {}. ' \
                        'You can contact https://t.me/{}.'.format(
        error_uid, config.Settings.support)
    return output


if __name__ == '__main__':
    app.run(host=config.Settings.domain,
            port=config.Settings.port,
            threaded=config.Settings.threaded)
