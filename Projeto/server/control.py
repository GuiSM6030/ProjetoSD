import bbs_pb2


def event_to_proto(event):
    payload = event["payload"]
    message = bbs_pb2.ReplicationEvent(
        event_id=event["event_id"],
        event_type=event["event_type"],
        origin=event["origin"],
    )
    fields = {
        "username": "username",
        "channel_name": "channel_name",
        "content": "content",
        "message_id": "message_id",
        "direction": "direction",
        "message_type": "message_type",
        "created_by": "created_by",
        "created_by_origin": "created_by_origin",
        "sent_at_unix_ms": "sent_at_unix_ms",
        "received_at_unix_ms": "received_at_unix_ms",
        "created_at_unix_ms": "created_at_unix_ms",
    }
    if "name" in payload:
        message.channel_name = payload["name"]
    for source, target in fields.items():
        if source in payload and payload[source] is not None:
            setattr(message, target, payload[source])
    return message


def event_from_proto(message):
    fields = {
        "username": message.username,
        "channel_name": message.channel_name,
        "content": message.content,
        "message_id": message.message_id,
        "direction": message.direction,
        "message_type": message.message_type,
        "created_by": message.created_by,
        "created_by_origin": message.created_by_origin,
        "sent_at_unix_ms": message.sent_at_unix_ms,
        "received_at_unix_ms": message.received_at_unix_ms,
        "created_at_unix_ms": message.created_at_unix_ms,
    }
    event_type = message.event_type
    if event_type == "channel":
        fields["name"] = message.channel_name
        fields.pop("channel_name")
    elif event_type == "publication":
        fields["channel_name"] = message.channel_name
    elif event_type == "exchange":
        fields["channel_name"] = message.channel_name or None
        fields["content"] = message.content or None
    else:
        fields["sent_at_unix_ms"] = message.sent_at_unix_ms
    return {
        "event_id": message.event_id,
        "event_type": event_type,
        "origin": message.origin,
        "payload": fields,
    }


def peer_request_to_proto(payload):
    request = bbs_pb2.PeerRequest(
        action=payload["action"],
        coordinator=payload.get("coordinator", ""),
        server_name=payload.get("server_name", ""),
        server_time_unix_ms=payload.get("server_time_unix_ms", 0),
    )
    request.events.extend(
        event_to_proto(event) for event in payload.get("events", [])
    )
    return request


def peer_request_from_proto(request):
    payload = {
        "action": request.action,
        "coordinator": request.coordinator,
        "server_name": request.server_name,
        "server_time_unix_ms": request.server_time_unix_ms,
        "events": [event_from_proto(event) for event in request.events],
    }
    return payload


def peer_response_to_proto(payload):
    response = bbs_pb2.PeerResponse(
        ok=payload.get("ok", False),
        detail=payload.get("detail", ""),
        server_name=payload.get("server_name", ""),
        rank=payload.get("rank", 0) or 0,
        server_time_unix_ms=payload.get("server_time_unix_ms", 0) or 0,
        correct_time_unix_ms=payload.get("correct_time_unix_ms", 0) or 0,
    )
    response.events.extend(
        event_to_proto(event) for event in payload.get("events", [])
    )
    response.applied_event_ids.extend(payload.get("applied", []))
    return response


def peer_response_from_proto(response):
    payload = {
        "ok": response.ok,
        "detail": response.detail,
        "server_name": response.server_name,
        "rank": response.rank,
        "server_time_unix_ms": response.server_time_unix_ms,
        "correct_time_unix_ms": response.correct_time_unix_ms,
        "events": [event_from_proto(event) for event in response.events],
        "applied": list(response.applied_event_ids),
    }
    return payload
