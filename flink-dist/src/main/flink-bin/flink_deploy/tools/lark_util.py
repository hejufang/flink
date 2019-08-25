# -*- coding: utf-8 -*-
# !/usr/bin/env python

from http_util import HttpUtil

URL_PREFIX = "https://oapi.zjurl.cn/open-apis/api"


class LarkUtils(object):
    def __init__(self, bot_token):
        self.bot_token = bot_token

    def get_channels_info(self, channel):
        req = {
            "token": self.bot_token,
            "channel": channel
        }
        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v1/channels.info", json=req)
        if succ:
            return resp.get("channel")
        else:
            return None

    def open_im(self, user_id):
        req = {
            "token": self.bot_token,
            "user": user_id
        }
        (succ, resp) = HttpUtil.do_post(URL_PREFIX + "/v1/im.open", json=req)
        if succ:
            return resp.get("channel").get("id")
        else:
            return None

    def send_message_to_user_list(self, user_name_list, msgs):
        if isinstance(user_name_list, list):
            for user_name in user_name_list:
                self.send_message(user_name, msgs)

    def send_message(self, user_name, msgs):
        user_id = self.get_user_id_from_name(user_name)
        im_id = self.open_im(user_id)
        self.post_message(im_id, msgs)

    def post_message(self, channel, message, at_user_ids=None,
                     at_user_names=None,
                     front=True, at_all=False):
        if at_all:
            at_msg = '<at user_id="all">@所有人</at>'
        else:
            at_msg = ""
            if at_user_ids and len(at_user_ids) > 0:
                for user_id in at_user_ids:
                    at_msg = at_msg + '<at user_id="%s">@%s</at>' % \
                                      (str(user_id),
                                       self.get_user_info(user_id))
            if at_user_names and len(at_user_names) > 0:
                for user_name in at_user_names:
                    at_msg = at_msg + '<at user_id="%s">@%s</at>' % \
                                      (self.get_user_id(
                                          user_name + "@bytedance.com"),
                                       user_name)

        if front:
            if at_msg:
                message = at_msg + " " + message
        else:
            if at_msg:
                message = message + " " + at_msg

        req = {
            "token": self.bot_token,
            "channel": channel,
            "text": message
        }
        (succ, resp) = HttpUtil.do_post(URL_PREFIX + "/v1/chat.postMessage",
                                        json=req)
        if succ:
            return resp
        else:
            return None

    def get_group_list(self, user_token=None):
        if not user_token:
            req = {
                "token": self.bot_token
            }
        else:
            req = {
                "token": user_token
            }

        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v1/groups.list", json=req)
        if succ:
            return resp.get("groups")
        else:
            return None

    def get_user_info(self, user_id):
        req = {
            "token": self.bot_token,
            "user": user_id
        }
        (succ, resp) = HttpUtil.do_post(URL_PREFIX + "/v1/user.info", json=req)
        if succ:
            return resp.get("name")
        else:
            return None

    def get_user_id(self, email):
        req = {
            "token": self.bot_token,
            "email": email
        }
        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v1/user.user_id", json=req)
        if succ:
            return resp.get("user_id")
        else:
            return None

    def get_user_id_from_name(self, name):
        req = {
            "token": self.bot_token,
            "email": name + "@bytedance.com"
        }
        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v1/user.user_id", json=req)
        if succ:
            return resp.get("user_id")
        else:
            return None

    def join_chat(self, user_token, chat_id):
        req = {
            "token": user_token,
            "bot": self.bot_token,
            "chat_id": chat_id
        }
        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v2/bot/chat/join", json=req)
        return succ

    def add_chat_members(self, chat_id, user_ids):
        req = {
            "token": self.bot_token,
            "chat_id": chat_id,
            "user_ids": user_ids
        }
        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v2/chat/member/add", json=req)
        return succ

    def create_chat(self, name, user_ids, icon_key=None):
        req = {
            "token": self.bot_token,
            "user_ids": user_ids,
            "name": name
        }
        if icon_key:
            req["icon_key"] = icon_key
        (succ, resp) = \
            HttpUtil.do_post(URL_PREFIX + "/v2/chat/create", json=req)
        if succ:
            return resp.get("chat").get("chat_id")
        else:
            return None
