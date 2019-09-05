# микро-сервис логирования
from utils.database import pymssql_proc_call, conn_str#, automap_tables, db_session

from flask import Flask, request, Response, jsonify, make_response
from flask import abort

from flask_sqlalchemy import SQLAlchemy

from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

logapp = Flask(__name__)
logapp.config['SQLALCHEMY_DATABASE_URI'] = conn_str
db = SQLAlchemy(logapp)

# префикс названия функции которая может вызыватьcя к метод API
method_prefix = 'api__'
# meta = db.metadata
# engine = db.engine
# https://charleslavery.com/notes/sqlalchemy-reflect-tables-to-declarative.html
def reflect_all_tables_to_declarative(uri, table_list=[]):
    """Reflects all tables to declaratives

    Given a valid engine URI and declarative_base base class
    reflects all tables and imports them to the global namespace.

    Returns a session object bound to the engine created.
    """

    # create an unbound base our objects will inherit from
    Base = declarative_base()

    # engine = create_engine(uri)
    engine = db.engine
    # metadata = MetaData(bind=engine)
    metadata = db.metadata
    Base.metadata = metadata

    g = globals()

    table_list=[table_list] if isinstance(table_list, str) else table_list
    metadata.reflect(engine, only=table_list)

    for tablename, tableobj in metadata.tables.items():
        g[tablename] = type(str(tablename), (Base,), {'__table__' : tableobj })
        print("Reflecting {0}".format(tablename))

    Session = sessionmaker(bind=engine)
    return Session()

db_session = reflect_all_tables_to_declarative(conn_str, 'Orders')




import logging
from logging.handlers import RotatingFileHandler



from py2db_err_codes import logger2group_err_mapping, py2db_err_codes_mapping



_LOGFILESZ = 1024*1024*5
rotating_handler = RotatingFileHandler(f"logs/{__file__}.log", maxBytes=_LOGFILESZ, backupCount=5)
rotating_handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(funcName)-20s: %(message)s'
))
# if not logapp.debug:
    # logapp.logger.addHandler(rotating_handler)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(rotating_handler)

# logger.critical("Test mes")
# logger.info("Test mes")




@logapp.route("/") 
def hello():
    return "Hello from log microservis!"


@logapp.route('/log/<string:methodsubname>', methods=['POST', 'GET'])
def log_router(methodsubname):
    logger.debug(f"Запрос к /log/{methodsubname}")

    api_method_name = f"{method_prefix}{methodsubname}"
    logger.debug(f"Ищем метод '{api_method_name}'")

    if api_method_name not in globals():        abort(405)

    callfunc = globals()[api_method_name]
    if not callable(callfunc):                  abort(403)
    logger.debug(f"Найден метод '{api_method_name}'")


    if request.method == 'POST':
        args = request.form if request.form != {} else request.json
    if request.method == 'GET':
        args = request.args

    if not args:                                abort(400)

    fname = callfunc.__name__
    logger.info(f"Вызываем метод: {fname}({repr(list(args.keys()))})")
    try:
        res = callfunc(args)
    except Exception as e:
        logger.critical(f"Ошибка! метод:{fname}({repr(args)}), исключение:{str(e)}, при вызове:{request.method} /log/{methodsubname}")
        
        r = {
            'status': 'ERROR'
            ,'webmethod': request.method
            ,'apimethod': f'/log/{methodsubname}'
            ,'args': repr(args)
            ,'error': str(e)
        }
        status=500

        # abort(500)
    else:
        logger.info(f"Успешно, результат вызова: '{repr(res)[:255]}'(размер:{len(repr(res))})")

        r = {
            'status': 'OK'
            ,'webmethod': request.method
            ,'apimethod': f'/log/{methodsubname}'
            ,'args': f"(первые 255 символов):'{repr(args)[:255]}'(общий размер:{len(repr(args))})"
            ,'result': repr(res)
        }
        status=200

    finally:
        return make_response(jsonify(r), status)


def api__aaa(args):
    """
        метод доступен по адресу localhost:PORT/log/aaa
        на вход получает args из запроса к api
        возвращает результат в виде строки
    """
    return f"{list(args.keys())}"

    if request.method == 'GET':
        s = "микро-сервис логирования\n"
        s += str(globals())
        return s
    j = request.json
    return str(j)
    return str(dir(request))


def api__sandbox_db(args):
    orderno=args.get('orderno',0)
    # костыли да грабли - в некоторых логгерах укахал не "orderno" a "order"
    orderno=args.get('order',0) if orderno == 0 else orderno

    supplier_id=args.get('supplier_id',0)
    supplier_id = get_supplierid_by_orderno(orderno) if orderno!=0 and supplier_id==0 else supplier_id

    msg_text=args.get('message')
    levelno=args.get('levelno')
    levelname=args.get('levelname')
    loggername=args.get('name')
    group_err = logger2group_err_mapping.get(loggername, 0)

    # py2db_err_codes_mapping: ключ==кортеж(название питон-логгера, название лог-уровня)      значение==код ошибки для логгера БД
    tuple_key = (loggername, levelname,)
    level_err = py2db_err_codes_mapping.get(tuple_key,0) 

    logger.info(f"group_err={group_err}, orderno={orderno}, supplier_id={supplier_id}, levelname={levelname}, loggername={loggername}, tuple_key={tuple_key}, msg_text={msg_text}")

    r ={
        'level_err'   :level_err, 
        'msg_text'    :msg_text, 
        'supplier_id' :supplier_id, 
        'orderno'     :orderno, 
        'group_err'   :group_err
    }
    log_into_db(**r)

    return r



@logapp.route("/test", methods=['POST', 'GET'])
def log_test():
    if request.method == 'GET':
        args = request.args
    if request.method == 'POST':
        args = request.form if request.form != {} else request.json
    

    logger.debug(list(args.keys()))

    return jsonify(api__sandbox_db(args))

    # for key, value in request.args.items():
        # #print(key,value)
        # s += f"{key}:\t{value[:255]}\n"
    # print(s)
    # s=""
    # return s



def log_into_db(level_err=0, msg_text="Тестовая запись", supplier_id=0, orderno=0, group_err=0):
    #dbo.update_log_Logging_Import_Problems @Order_ID,115,@Message,@Supplier_ID
    if level_err==0 and supplier_id==0 and orderno==0:
        log.warning(f"Лог-событие не внесено в БД - пустые параметры!")
        return

    try:
        pymssql_proc_call("dbo.update_log_Logging_Import_Problems", orderno, level_err, msg_text, supplier_id, group_err)
    except Exception as e:
        logger.error(f"{e}; dbo.update_log_Logging_Import_Problems({orderno}, {level_err}, {msg_text}, {supplier_id}, {group_err})")
        pymssql_proc_call("dbo.update_log_Logging_Import_Problems", 0, 0, str(e), 0, 0) # пишем в БДлог текст эксепшн с типом Неизвестная ошибка
    else:
        logger.info(f"dbo.update_log_Logging_Import_Problems({orderno}, {level_err}, {msg_text}, {supplier_id}, {group_err})")



def get_supplierid_by_orderno(orderno):
    # exists = db_session.query(Orders.ID).filter(Orders.Is_Deleted==0, Orders.ID==orderno).scalar() is not None
    # if exists:
        # supplier_id, = db_session.query(Orders.Supplier_ID).filter(Orders.Is_Deleted==0, Orders.ID==orderno).first()
        # return supplier_id

    # res_tuple = db_session.query(Orders.Supplier_ID).filter(Orders.Is_Deleted==0, Orders.ID==orderno).first()
    res_tuple = db_session.query(Orders.Supplier_ID).filter(Orders.ID==orderno).first() # будем возвращать Supplier_ID даже для удаленных заказов
    logger.debug(f"orderno:{orderno}, res_tuple:{repr(res_tuple)}")
    return res_tuple[0] if res_tuple is not None or res_tuple!=tuple() else 0




# print(dir(Orders))
# print([c.name for c in Orders.columns])
# print(get_supplierid_by_orderno(15155))

if __name__ == "__main__":
  # logapp.run(port=8080)
  # logapp.run(debug=True)
    logapp.run()
  
