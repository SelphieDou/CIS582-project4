from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)


@app.before_request
def create_session():
    g.session = scoped_session(DBSession)


@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """


def check_sig(payload, sig):
    platform = payload["platform"]
    sender_pk = payload["sender_pk"]
    sig_right = False
    # check sig
    if platform == "Ethereum":
        msg = json.dumps(payload)
        eth_encoded_msg = eth_account.messages.encode_defunct(text=msg)
        get_account = eth_account.Account.recover_message(signable_message=eth_encoded_msg, signature=sig)
        if sender_pk == get_account:
            sig_right = True
    if platform == "Algorand":
        msg = json.dumps(payload)
        if algosdk.util.verify_bytes(msg.encode('utf-8'), sig, sender_pk):
            sig_right = True
    return sig_right

def fill_order(order, txes=[]):
    result = get_all_match_orders(order)
    if len(result) > 0:
        sorted(result, key=lambda o: o.sell_amount, reverse=True)
        existing_order = result[0]
        # Set the filled field to be the current timestamp on both orders
        current_time = datetime.now()
        existing_order.filled = current_time
        order.filled = current_time
        # Set counterparty_id to be the id of the other order
        order.counterparty_id = existing_order.id
        existing_order.counterparty_id = order.id
        # Create a new order for remaining balance
        new_order = None
        if existing_order.buy_amount > order.sell_amount:
            new_order = Order()
            differ = existing_order.buy_amount - order.sell_amount
            new_order.buy_amount = differ
            sell_amount = differ * existing_order.sell_amount / existing_order.buy_amount
            new_order.sell_amount = sell_amount
            new_order.creator_id = existing_order.id
            new_order.sell_currency = existing_order.sell_currency
            new_order.buy_currency = existing_order.buy_currency
            new_order.receiver_pk = existing_order.receiver_pk
            new_order.sender_pk = existing_order.sender_pk
        if existing_order.buy_amount < order.sell_amount:
            new_order = Order()
            differ = order.sell_amount - existing_order.buy_amount
            new_order.sell_amount = differ
            buy_amount = differ * order.buy_amount / order.sell_amount
            new_order.buy_amount = buy_amount
            new_order.creator_id = order.id
            new_order.sell_currency = order.sell_currency
            new_order.buy_currency = order.buy_currency
            new_order.receiver_pk = order.receiver_pk
            new_order.sender_pk = order.sender_pk
        if new_order != None:
            g.session().add(new_order)
        g.session().commit()


def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    obj = Log()
    for r in d.keys():
        obj.__setattr__(r, d[r])
    session = g.session()
    session.add(obj)
    session.commit()


def get_all_match_orders(order):
    """
    get all matched orders
    :param order:
    :return:list
    """
    # existing_order.buy_currency == order.sell_currency
    # existing_order.sell_currency == order.buy_currency
    # taker
    session = g.session()
    cur_res = order.buy_amount / order.sell_amount
    res = session.query(Order).filter(Order.filled == None, Order.buy_currency == order.sell_currency,
                                      Order.sell_currency == order.buy_currency).all()
    result = []
    if len(res) > 0:
        for obj in res:
            # maker
            tmp_res = obj.sell_amount / obj.buy_amount
            if tmp_res >= cur_res:
                result.append(obj)
    return result

def insert_order(payload,sig):
    session=g.session()
    order_dict = {}
    order_dict['sender_pk'] = payload['sender_pk']
    order_dict['receiver_pk'] = payload['receiver_pk']
    order_dict['buy_currency'] = payload['buy_currency']
    order_dict['sell_currency'] = payload['sell_currency']
    order_dict['buy_amount'] = payload['buy_amount']
    order_dict['sell_amount'] = payload['sell_amount']
    order_dict['signature'] = sig
    obj = Order()
    for r in order_dict.keys():
        obj.__setattr__(r, order_dict[r])
    session.add(obj)
    session.commit()
    return obj

""" End of helper methods """


@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        session = g.session()
        content = request.get_json(silent=True)
        print(f"content = {json.dumps(content)}")
        columns = ["sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform"]
        fields = ["sig", "payload"]

        for field in fields:
            if not field in content.keys():
                print(f"{field} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        for column in columns:
            if not column in content['payload'].keys():
                print(f"{column} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        # Your code here
        # Note that you can access the database session using g.session
        # TODO: Check the signature
        sig = content["sig"]
        payload = content["payload"]
        check_flag=check_sig(payload,sig)
        # TODO: Add the order to the database
        if (check_flag):
            order=insert_order(payload,sig)
        # TODO: Fill the order
            if order:
                fill_order(order,None)
            else:
                check_flag=False
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
        return jsonify(check_flag)

@app.route('/order_book')
def order_book():
    # Your code here
    # Note that you can access the database session using g.session
    result = {}
    session = g.session()
    data = session.query(Order).all()
    data_list = []
    for obj in data:
        order_dict = {}
        order_dict['sender_pk'] = obj.sender_pk
        order_dict['receiver_pk'] = obj.receiver_pk
        order_dict['buy_currency'] = obj.buy_currency
        order_dict['sell_currency'] = obj.sell_currency
        order_dict['buy_amount'] = obj.buy_amount
        order_dict['sell_amount'] = obj.sell_amount
        order_dict['signature'] = obj.signature
        data_list.append(order_dict)
    result["data"] = data_list
    return jsonify(result)


if __name__ == '__main__':
    app.run(port='5002')
