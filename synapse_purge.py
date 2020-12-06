#!/usr/bin/env python3

# by SoftwareSchlosser

import array as arr
import json
import os
import sys
import time
import psycopg2
import pprint
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dry", help="dry run", action="store_true")
parser.add_argument("-e", "--remove_message_events", help="removes message events from database (DONT USE UNLESS YOU KNOW EXACTLY WHAT YOU ARE DOING! THIS CAN MESS UP YOUR SERVER!)", action="store_true")
parser.add_argument("-m", "--remove_media", help="removes media from file system", action="store_true")
parser.add_argument("--hours", help="only purge older than n hours", type=int)
parser.add_argument("--days", help="only purge older than n days", type=int)
parser.add_argument("--media_size", help="only purge media greater than n MB", type=int)
parser.add_argument("--user_id", help="only purge for given user id", type=str)
parser.add_argument("--room_id", help="only purge for given room id", type=str)
parser.add_argument("--media_id", help="only purge media with given id", type=str)
parser.add_argument("--database_host", help="database host", type=str, default="127.0.0.1")
parser.add_argument("--database_port", help="database port", type=int, default=5432)
parser.add_argument("--database_name", help="database name", type=str, default="synapse")
parser.add_argument("--database_user", help="database user", type=str, default="synapse_user")
parser.add_argument("--database_password", help="database password", type=str, default="synapse_password")
parser.add_argument("--content_path", help="path to matrix synapse local content", type=str, default="/var/lib/matrix-synapse/media/local_content")
parser.add_argument("matrix_server", help="matrix server domain (e.g. matrix.domain.tld")
args = parser.parse_args()

SERVER_CONTENT_PATH = args.content_path
DB = psycopg2.connect(database=args.database_name, user=args.database_user, password=args.database_password, host=args.database_host, port=args.database_port)

def AddCondition(sConditions, sCondition):
  if (len(sConditions) > 0):
    return sConditions + " AND " + sCondition;
  else:
    return sCondition;

def GetQueryConditions(column_date, column_room_id, column_user_id, column_event_id, column_media_id):
  sConditions = ""
  if (column_date is not None):
    nHours = 0
    if (args.days is not None):
      nHours = args.days * 24
    if (args.hours is not None):
      nHours = nHours + args.hours
    sConditions = AddCondition(sConditions, f"{column_date} < {int(round((time.time()-(nHours*60*60)) * 1000))}")

  if (column_user_id is not None and args.user_id is not None):
    sConditions = AddCondition(sConditions, f"{column_user_id} = '{args.user_id}'")

  if (column_room_id is not None and args.room_id is not None):
    sConditions = AddCondition(sConditions, f"{column_room_id} = '{args.room_id}'")

  if (column_media_id is not None and args.media_id is not None):
    sConditions = AddCondition(sConditions, f"{column_media_id} = '{args.media_id}'")

  return sConditions

def DeleteMediaFileByID(media_id):
  parts = media_id.split("/")
  sMediaID = parts[len(parts)-1]
  sMediaPath = SERVER_CONTENT_PATH + "/" + sMediaID[0:2] + "/" + sMediaID[2:4] + "/" + sMediaID[4:]

  if os.path.exists(sMediaPath):
    print(f"deleting file: \"{sMediaPath}\"")
    os.remove(sMediaPath)
  else:
    print(f"file not found: \"{sMediaPath}\"")

def RemoveEventsByIdList(event_id_list):
  hCursorDelete = DB.cursor()

  hCursorDelete.execute("DELETE FROM events WHERE room_id = %(roomId)s AND event_id IN %(eventsList)s", { "roomId": room_id, "eventsList": tuple(delete_event_id_list), })
  hCursorDelete.execute("DELETE FROM event_json WHERE room_id = %(roomId)s AND event_id IN %(eventsList)s", { "roomId": room_id, "eventsList": tuple(delete_event_id_list), })
  hCursorDelete.execute("DELETE FROM event_push_actions WHERE room_id = %(roomId)s AND event_id IN %(eventsList)s", { "roomId": room_id, "eventsList": tuple(delete_event_id_list), })

  hCursorDelete.execute("DELETE FROM event_edges WHERE room_id = %(roomId)s AND event_id IN %(eventsList)s", { "roomId": room_id, "eventsList": tuple(delete_event_id_list), })
  hCursorDelete.execute("DELETE FROM event_reference_hashes WHERE event_id IN %(eventsList)s", { "eventsList": tuple(delete_event_id_list), })
  hCursorDelete.execute("DELETE FROM event_to_state_groups WHERE event_id IN %(eventsList)s", { "eventsList": tuple(delete_event_id_list), })

  DB.commit()

def RemoveEvents():
  hCursor = DB.cursor()
  sQuery = f"SELECT e.event_id, e.room_id, e.content, ej.json \
 FROM events e \
 JOIN event_json ej ON ej.event_id = e.event_id and ej.room_id = e.room_id \
 WHERE e.contains_URL=TRUE AND " + GetQueryConditions("e.origin_server_ts", "e.room_id", "e.sender", "e.event_id", None)

  hCursor.execute(sQuery)

  rows = hCursor.fetchall()

  delete_event_id_list = []

  for row in rows:
    (event_id, room_id, content, json_string) = row
    js = json.loads(json_string)
    if (js["type"] == "m.room.message"):
#      if (js["content"]["msgtype"] == "m.image" and js["content"]["url"]):
      #print("event_id: " + event_id)
      delete_event_id_list.append(event_id)
  hCursor.close()

  if (len(delete_event_id_list) > 0):
    print("clearing events:")
    pprint.pprint(delete_event_id_list)
    if (args.dry == False):
      RemoveEventsByIdList(delete_event_id_list)

  DB.close();

def FindEventsByMediaID(hCursor, media_id):
  hCursor.execute(f"SELECT event_id, json FROM event_json WHERE json LIKE '%mxc://{args.matrix_server}/{media_id}%'")
  rows = hCursor.fetchall()
  events = []
  for row in rows:
    js = json.loads(row[1])
    if ("event_id" in js):
      events.append(js["event_id"])
  return events

def RemoveMedia():
  delete_media_id_list = []
  delete_event_id_list = []

  hCursor = DB.cursor()
  queryParams = { }
  sQuery = f"SELECT lmr.media_id, lmr.media_length, lmr.media_type, lmr.created_ts, lmr.last_access_ts, lmr.upload_name, lmr.user_id FROM local_media_repository lmr \
  WHERE " + GetQueryConditions("lmr.created_ts", None, "lmr.user_id", None, "lmr.media_id")
  
  if (args.media_size is not None):
    sQuery = sQuery + " AND media_length > %(_media_length)s"
    queryParams['_media_length'] = args.media_size * 1024 * 1024

  hCursor.execute(sQuery, queryParams)
  rows = hCursor.fetchall()
  for row in rows:
    (media_id, media_length, media_type, created_ts, last_access_ts, upload_name, user_id) = row
    if created_ts is None:
      created_time = datetime.fromtimestamp(0)
    else:
      created_time = datetime.fromtimestamp(created_ts / 1000)
    if last_access_ts is None:
      last_access_time = datetime.fromtimestamp(0)
    else:
      last_access_time = datetime.fromtimestamp(last_access_ts / 1000)

    events = FindEventsByMediaID(hCursor, media_id)

    print(f"media {media_id} size {round(media_length / 1024 / 1024, 2)}MB, upload name {upload_name}, media type {media_type}, user {user_id}, created {created_time.strftime('%d.%m.%Y %H:%M:%S')}, last access {last_access_time.strftime('%d.%m.%Y %H:%M:%S')}, used in {len(events)} events")

    if (len(events) > 0):
      delete_event_id_list.extend(events)

    if (args.dry == False):
      delete_media_id_list.append(media_id)
      DeleteMediaFileByID(media_id)

  if (len(delete_media_id_list) > 0):
    print("clearing media data...")
    hCursor.execute("DELETE FROM local_media_repository_thumbnails WHERE media_id IN %(mediaList)s", { "mediaList": tuple(delete_media_id_list), })
    hCursor.execute("DELETE FROM local_media_repository WHERE media_id IN %(mediaList)s", { "mediaList": tuple(delete_media_id_list), })

  if (len(delete_event_id_list) > 0 and args.remove_message_events == True):
    print("clearing media events:")
    pprint.pprint(delete_media_id_list)
    RemoveEventsByIdList(delete_event_id_list)

  DB.commit()
  hCursor.close()

if (args.remove_media == True):
  RemoveMedia()
elif (args.remove_message_events == True):
  RemoveEvents()
