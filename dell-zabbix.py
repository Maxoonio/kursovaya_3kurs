#!/usr/bin/env python3
import sys
import json
import logging
import base64
from typing import Any, Dict, List, Union, Optional
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

class ParamError(Exception):
    pass

def validate_zabbix_params(required_params: List[str], all_params: Dict[str, Any]) -> None:
    for field in required_params:
        if not isinstance(all_params, dict) or field not in all_params or all_params[field] in ("", None):
            raise ParamError(f"Required param is not set: {field}.")

class HttpClient:
    def __init__(self, params: Dict[str, Any]):
        self.params = params
        self.session = requests.Session()
        self._prepare_auth()
        self._prepare_proxy()

    def _prepare_auth(self):
        user = self.params.get("user")
        password = self.params.get("password")
        if user is not None and password is not None:
            # We'll use requests' auth or Authorization header depending on needs
            self.session.auth = (user, password)

    def _prepare_proxy(self):
        proxy = self.params.get("http_proxy")
        if proxy:
            # requests expects proxies dict like {"http": "...", "https": "..."}
            self.session.proxies = {"http": proxy, "https": proxy}
            logging.info("Using http proxy: %s", proxy)

    def get(self, url: str, expected_status: Optional[int] = None, timeout: int = 10) -> str:
        headers = {}
        # If explicit Basic header requested (some environments), support header style:
        if self.params.get("force_basic_header"):
            user = self.params.get("user", "")
            password = self.params.get("password", "")
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"

        try:
            resp = self.session.get(url, headers=headers, timeout=timeout, verify=self.params.get("verify_ssl", True))
        except Exception as e:
            logging.error("HTTP request to %s failed: %s", url, e)
            raise

        status_expected = None
        if expected_status is not None:
            try:
                status_expected = int(expected_status)
            except Exception:
                status_expected = None

        if status_expected is not None:
            if resp.status_code == status_expected:
                return resp.text
            else:
                logging.error("Unexpected HTTP response code: %s (expected %s) for %s", resp.status_code, status_expected, url)
                raise RuntimeError(f"Unexpected HTTP response code: {resp.status_code}")
        else:
            if resp.ok:
                return resp.text
            else:
                logging.error("HTTP request returned non-ok status %s for %s", resp.status_code, url)
                raise RuntimeError(f"HTTP error {resp.status_code}")


class Dell:
    def __init__(self, service: str, action: str, params: Dict[str, Any]):
        self.zabbix_log_prefix = f"[ DELL ] [ {service} ] [ {action} ]"
        self.params = params
        self.client = HttpClient(params)

    def execute_request(self, api_path: str) -> str:
        base = self.params.get("url", "").rstrip("/")
        url = base + api_path
        expected_code = self.params.get("http_status_code")
        return self.client.get(url, expected_status=expected_code)

    def extract_data(self, data: Union[str, Dict[str, Any], List[Any]], keys: Union[List[Any], Dict[str, Any], str], skip_parse: bool=False) -> Any:
        if not skip_parse and isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception as e:
                logging.error("%s Could not parse the received JSON object: %s", self.zabbix_log_prefix, e)
                raise RuntimeError("Could not parse received JSON object. See logs for more information.")

        if isinstance(keys, list):
            return_buffer: Dict[str, Any] = {}
            for key in keys:
                name = key.get("name")
                path = key.get("path")
                if path == "@odata.id":
                    # direct field
                    return_buffer[name] = data.get(path)
                else:
                    return_buffer[name] = self.parse_json(data, path.split("."))
            return return_buffer
        elif isinstance(keys, dict):
            name = keys.get("name")
            path = keys.get("path")
            return { name: self.parse_json(data, path.split(".")) }
        elif isinstance(keys, str):
            return self.parse_json(data, keys.split("."))
        else:
            raise RuntimeError("Unexpected key type")

    def parse_json(self, data: Any, json_path: List[str]) -> Any:
        current = data
        for p in json_path:
            if current is None:
                return None
            # If path element looks like an index, attempt int index
            try:
                # handle array index like 'Members[0]' or plain keys
                if p.endswith("]") and "[" in p:
                    # split e.g. Members[0] -> Members and 0
                    key_part, idx_part = p[:-1].split("[", 1)
                    current = current.get(key_part)
                    idx = int(idx_part)
                    current = current[idx]
                else:
                    if isinstance(current, dict):
                        current = current.get(p)
                    elif isinstance(current, list):
                        # if current is list and p is integer index
                        try:
                            idx = int(p)
                            current = current[idx]
                        except Exception:
                            # cannot access, return None
                            return None
                    else:
                        return None
            except Exception:
                return None
        return current

def format_psu(psu_obj: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    found_power_supplies = []
    buffer = []
    for obj in psu_obj:
        psu_name = obj.get("name", "").split(" ")[0]
        if psu_name not in found_power_supplies:
            found_power_supplies.append(psu_name)
            buffer.append({
                "name": psu_name,
                obj.get("type", "").lower(): {"reading": obj.get("reading"), "health": obj.get("health")}
            })
        else:
            for b in buffer:
                if b["name"] == psu_name:
                    b[obj.get("type", "").lower()] = {"reading": obj.get("reading"), "health": obj.get("health")}
                    break
    return buffer

def format_temp(temp_obj: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for obj in temp_obj:
        name = obj.get("name", "")
        parts = name.split(" ")
        if len(parts) > 1:
            obj["name"] = " ".join(parts[:-1])
    return temp_obj

def format_sysboard(sys_obj: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    remove = ['System', 'Board', 'Usage']
    for obj in sys_obj:
        name = obj.get("name", "")
        words = name.split(" ")
        buffer = [w for w in words if w not in remove]
        if buffer:
            obj["name"] = " ".join(buffer)
    return sys_obj

def main():
    # Read params JSON from stdin or first CLI argument
    raw = None
    if not sys.stdin.isatty():
        try:
            raw = sys.stdin.read()
            if raw.strip() == "":
                raw = None
        except Exception:
            raw = None

    if raw is None and len(sys.argv) > 1:
        raw = sys.argv[1]

    if raw is None:
        logging.error("No input parameters provided. Expecting JSON on stdin or as first argument.")
        sys.exit(2)

    try:
        params = json.loads(raw)
    except Exception as e:
        logging.error("Failed to parse params JSON: %s", e)
        sys.exit(2)

    try:
        validate_zabbix_params(['url', 'user', 'password', 'http_status_code'], params)
    except ParamError as e:
        logging.error(str(e))
        sys.exit(2)

    # First: system-level requests (like original JS)
    try:
        dell_system = Dell('System', 'Get system metrics', params)
        requests_list = [
            {
                'path': '/redfish/v1/Systems/System.Embedded.1',
                'keys': [
                    {'name': 'model', 'path': 'Model'},
                    {'name': 'serialnumber', 'path': 'Oem.Dell.DellSystem.ChassisServiceTag'},
                    {'name': 'status', 'path': 'Status.Health'}
                ]
            },
            {
                'path': '/redfish/v1/Managers/iDRAC.Embedded.1',
                'keys': {'name': 'firmware', 'path': 'FirmwareVersion'}
            }
        ]

        buffer: Dict[str, Any] = {}
        for req in requests_list:
            try:
                resp_text = dell_system.execute_request(req['path'])
                extracted = dell_system.extract_data(resp_text, req['keys'])
                buffer.update(extracted)
            except Exception as e:
                logging.warning("Request %s failed: %s", req.get('path'), e)
        # Print JSON result for this part (if only this is needed)
        # But we continue to sensors discovery below and merge results into buffer
    except Exception as e:
        logging.warning("System metrics collection failed: %s", e)
        buffer = {}

    # Second: sensors discovery (the longer script)
    try:
        dell_sensors = Dell('Sensor', 'Discovery', params)
        sensors_raw = dell_sensors.execute_request('/redfish/v1/Chassis/System.Embedded.1/Sensors?$expand=.($levels=1)')
        # sensors_raw might be JSON with Members array OR could be an object; try to parse and get 'Members'
        try:
            sensors_parsed = json.loads(sensors_raw)
        except Exception:
            sensors_parsed = None

        sensors = None
        if isinstance(sensors_parsed, dict):
            sensors = sensors_parsed.get('Members') or sensors_parsed.get('members') or sensors_parsed
        # If execute_request returned a list or string, try to treat as JSON array
        if sensors is None:
            try:
                sensors = json.loads(sensors_raw)
            except Exception:
                sensors = None

        if not isinstance(sensors, list):
            raise RuntimeError("Unexpected type for sensors.")

        keys = [
            {'name': 'id', 'path': 'Id'},
            {'name': 'name', 'path': 'Name'},
            {'name': 'context', 'path': 'PhysicalContext'},
            {'name': 'reading', 'path': 'Reading'},
            {'name': 'type', 'path': 'ReadingType'},
            {'name': 'health', 'path': 'Status.Health'}
        ]

        out = {'fan': [], 'psu': [], 'temperature': [], 'sysBoard': []}

        # sensors may contain either member references or full objects. If member refs, we need to fetch each.
        for sensor in sensors:
            # If sensor looks like a reference (dict with '@odata.id' or similar), fetch it
            if isinstance(sensor, dict) and ('@odata.id' in sensor or 'Members' in sensor and isinstance(sensor.get('Members'), list)):
                # If sensor has '@odata.id' then we need to GET that path
                if isinstance(sensor, dict) and '@odata.id' in sensor:
                    sensor_path = sensor['@odata.id']
                    try:
                        sensor_text = dell_sensors.execute_request(sensor_path)
                        sensor_obj = json.loads(sensor_text)
                    except Exception as e:
                        logging.warning("Failed to fetch sensor %s: %s", sensor_path, e)
                        continue
                else:
                    sensor_obj = sensor
            elif isinstance(sensor, str):
                # If sensor is a URL string, fetch it
                try:
                    sensor_text = dell_sensors.execute_request(sensor)
                    sensor_obj = json.loads(sensor_text)
                except Exception as e:
                    logging.warning("Failed to fetch sensor %s: %s", sensor, e)
                    continue
            elif isinstance(sensor, dict):
                sensor_obj = sensor
            else:
                logging.warning("Unknown sensor entry type, skipping: %s", type(sensor))
                continue

            # Normalize sensor object: if it contains 'Members', skip (already handled)
            # Extract fields according to keys
            try:
                parsed = dell_sensors.extract_data(sensor_obj, keys, skip_parse=True)
            except Exception as e:
                logging.warning("Failed to parse sensor object: %s", e)
                continue

            reading_type = parsed.get('type')
            name = parsed.get('name', '')

            if isinstance(name, str) and name.startswith('PS') and parsed.get('context') == 'PowerSupply':
                out['psu'].append(parsed)
            elif reading_type == 'Temperature':
                out['temperature'].append(parsed)
            elif isinstance(name, str) and 'Fan' in name and reading_type == 'Rotational':
                out['fan'].append(parsed)
            elif isinstance(name, str) and 'System Board' in name and reading_type == 'Percent':
                out['sysBoard'].append(parsed)
            else:
                # If nothing matched, try to classify by PhysicalContext
                ctx = parsed.get('context', '')
                if ctx == 'PowerSupply':
                    out['psu'].append(parsed)
                elif ctx == 'Chassis':
                    out['sysBoard'].append(parsed)
                else:
                    # ignore unknown sensors
                    pass

        out['psu'] = format_psu(out['psu'])
        out['temperature'] = format_temp(out['temperature'])
        out['sysBoard'] = format_sysboard(out['sysBoard'])

        # Merge with previously collected buffer (system info)
        if isinstance(buffer, dict):
            buffer.update(out)
        else:
            buffer = out

    except Exception as e:
        logging.warning("Sensors discovery failed: %s", e)
        # If sensors fail, still print what we have
        if not buffer:
            buffer = {}

    # Final: print JSON
    print(json.dumps(buffer, ensure_ascii=False))

if __name__ == "__main__":
    main()
