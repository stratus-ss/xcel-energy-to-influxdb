import requests
from datetime import datetime, timedelta
import json
from collections import defaultdict
import time

class EnphaseEnergyMonitor:
    def __init__(self, use_cloud=False, envoy_host='envoy.local', 
                 username=None, password=None, envoy_serial=None,
                 client_id=None, client_secret=None, bearer_token = None, system_id = None):
        self.use_cloud = use_cloud
        self.envoy_host = envoy_host
        self.username = username
        self.password = password
        self.envoy_serial = envoy_serial
        self.client_id = client_id
        self.client_secret = client_secret
        self.system_id = system_id
        self.token = bearer_token
        self.cloud_token = None
        self.token_expiration = 0
        
        if not use_cloud and self.token is None:
            self._get_local_token()
        elif use_cloud:
            self._get_cloud_token()

    def _get_cloud_token(self):
        """
        Guide user through OAuth2 authorization code flow for Enphase API.
        
        For automation, this function stores/retrieves tokens from a secure file.
        First-time use requires manual authorization.
        """
        token_file = 'enphase_token.json'
        
        # Try to load existing token
        try:
            with open(token_file, 'r') as f:
                token_data = json.load(f)
                
                # Check if token is expired
                if token_data.get('expires_at', 0) > time.time():
                    self.cloud_token = token_data['access_token']
                    self.token_expiration = token_data['expires_at']
                    return
                    
                # Try to use refresh token if available
                if 'refresh_token' in token_data:
                    try:
                        print("Refreshing expired token...")
                        token_url = 'https://api.enphaseenergy.com/oauth/token'
                        response = requests.post(
                            token_url,
                            data={
                                'grant_type': 'refresh_token',
                                'refresh_token': token_data['refresh_token'],
                                'client_id': self.client_id,
                                'client_secret': self.client_secret
                            }
                        )
                        
                        if response.status_code == 200:
                            new_token_data = response.json()
                            self.cloud_token = new_token_data['access_token']
                            self.token_expiration = time.time() + new_token_data['expires_in'] - 300
                            
                            # Save new token data
                            new_token_data['expires_at'] = self.token_expiration
                            with open(token_file, 'w') as f:
                                json.dump(new_token_data, f)
                                
                            return
                    except Exception as e:
                        print(f"Error refreshing token: {e}")
        except (FileNotFoundError, json.JSONDecodeError):
            pass
            
        # If we get here, we need manual authorization
        auth_url = f"https://api.enphaseenergy.com/oauth/authorize?response_type=code&client_id={self.client_id}"
        
        print("\nManual authorization required for Enphase API access.")
        print(f"\nPlease open this URL in your browser:\n{auth_url}")
        print("\nLog in with your Enphase credentials and authorize the application.")
        print("After authorization, you will be redirected to a page with an authorization code.")
        
        auth_code = input("\nEnter the authorization code: ")
        
        # Exchange auth code for tokens
        token_url = 'https://api.enphaseenergy.com/oauth/token'
        response = requests.post(
            token_url,
            data={
                'grant_type': 'authorization_code',
                'code': auth_code,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'redirect_uri': 'https://api.enphaseenergy.com/oauth/redirect_uri'
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to get access token: {response.text}")
            
        token_data = response.json()
        self.cloud_token = token_data['access_token']
        self.token_expiration = time.time() + token_data['expires_in'] - 300
        
        # Save token data for future use
        token_data['expires_at'] = self.token_expiration
        with open(token_file, 'w') as f:
            json.dump(token_data, f)



    def _refresh_cloud_token(self):
        """Refresh token if expired"""
        if time.time() > self.token_expiration:
            self._get_cloud_token()

    def _get_cloud_system_id(self):
        """Get first system ID associated with account"""
        self._refresh_cloud_token()
        systems_url = 'https://api.enphaseenergy.com/api/v4/systems'
        
        response = requests.get(
            systems_url,
            headers={'Authorization': f'Bearer {self.cloud_token}'}
        )
        
        if response.status_code == 200:
            systems = response.json()
            return systems['systems'][0]['system_id'] if systems['systems'] else None
        return None

    def _get_cloud_data(self):
        """Get current energy metrics from cloud API"""
        self._refresh_cloud_token()
        
        if not self.system_id:
            self.system_id = self._get_cloud_system_id()
            if not self.system_id:
                raise Exception("No Enphase system found in cloud account")

        summary_url = f'https://api.enphaseenergy.com/api/v4/systems/{self.system_id}/summary'
        consumption_url = f'https://api.enphaseenergy.com/api/v4/systems/{self.system_id}/consumption_stats'

        try:
            # Get production summary
            summary_response = requests.get(
                summary_url,
                headers={'Authorization': f'Bearer {self.cloud_token}'},
                timeout=10
            )
            summary_response.raise_for_status()
            summary = summary_response.json()

            # Get consumption data
            consumption_response = requests.get(
                consumption_url,
                headers={'Authorization': f'Bearer {self.cloud_token}'},
                timeout=10
            )
            consumption = consumption_response.json() if consumption_response.status_code == 200 else {}

            return self._parse_cloud_data(summary, consumption)
        except requests.exceptions.RequestException as e:
            print(f"Cloud API Error: {str(e)}")
            return None

    def _parse_cloud_data(self, summary, consumption):
        """Parse cloud API response into standard metrics format"""
        metrics = {
            "timestamp": int(time.time()),
            "import_lifetime_wh": summary.get('grid_imported_energy', 0) * 1000,
            "export_lifetime_wh": summary.get('grid_exported_energy', 0) * 1000,
            "import_power_w": consumption.get('grid_import_power', 0),
            "export_power_w": consumption.get('grid_export_power', 0),
            "production_power_w": summary.get('current_power', 0),
            "consumption_power_w": consumption.get('total_consumption_power', 0),
            "battery_charge_w": summary.get('storage_charge_power', 0),
            "battery_discharge_w": summary.get('storage_discharge_power', 0),
            "meters": []
        }

        # Add inverter-level data if available
        if 'envoys' in summary:
            for envoy in summary['envoys']:
                metrics["meters"].append({
                    "id": envoy.get('serial_number', 'unknown'),
                    "production_w": envoy.get('current_power', 0),
                    "status": envoy.get('status', 'unknown')
                })

        return metrics


    def _get_local_token(self):
        """Obtain JWT token for local API access"""
        login_url = 'https://enlighten.enphaseenergy.com/login/login.json'
        token_url = 'https://entrez.enphaseenergy.com/tokens'
        
        session = requests.Session()
        response = session.post(login_url, data={
            'user[email]': self.username,
            'user[password]': self.password
        })
        
        session_id = response.json().get('session_id')
        response = session.post(token_url, json={
            'session_id': session_id,
            'serial_num': self.envoy_serial,
            'username': self.username
        })
        
        self.token = response.text.strip('"')

    def get_meter_data(self):
        """Get current energy metrics from local API"""
        if self.use_cloud:
            return self._get_cloud_data()
            
        url = f'http://{self.envoy_host}/ivp/meters/readings'
        headers = {'Authorization': f'Bearer {self.token}'}
        
        try:
            response = requests.get(url, headers=headers, timeout=10, verify=False)
            response.raise_for_status()
            return self._parse_meter_data(response.json())
        except requests.exceptions.RequestException as e:
            print(f"API Error: {str(e)}")
            return None

    def _parse_meter_data(self, data):
        """Parse local API meter response"""
        metrics = {
            "timestamp": int(time.time()),
            "import_lifetime_wh": 0,   # Total energy imported (actEnergyDlvd)
            "export_lifetime_wh": 0,   # Total energy exported (actEnergyRcvd)
            "import_power_w": 0,       # Current import power (positive activePower)
            "export_power_w": 0,       # Current export power (negative activePower)
            "reactive_import_varh": 0,  # Reactive energy import (reactEnergyLagg)
            "reactive_export_varh": 0,  # Reactive energy export (reactEnergyLead)
            "meters": []               # Individual meter data
        }
        
        # Process each meter
        for meter in data:
            meter_timestamp = meter.get("timestamp", 0)
            meter_id = meter.get("eid", "unknown")
            
            # Extract key values from your data structure
            imported_wh = float(meter.get("actEnergyDlvd", 0))
            exported_wh = float(meter.get("actEnergyRcvd", 0))
            active_power = float(meter.get("activePower", 0))
            reactive_import = float(meter.get("reactEnergyLagg", 0))
            reactive_export = float(meter.get("reactEnergyLead", 0))
            
            # Store individual meter data
            meter_data = {
                "id": meter_id,
                "timestamp": meter_timestamp,
                "imported_wh": imported_wh,
                "exported_wh": exported_wh,
                "active_power_w": active_power,
                "voltage": float(meter.get("voltage", 0)),
                "current": float(meter.get("current", 0)),
                "reactive_import_varh": reactive_import,
                "reactive_export_varh": reactive_export,
                "pwr_factor": float(meter.get("pwrFactor", 0))
            }
            metrics["meters"].append(meter_data)
            
            # Accumulate totals
            metrics["import_lifetime_wh"] += imported_wh
            metrics["export_lifetime_wh"] += exported_wh
            metrics["reactive_import_varh"] += reactive_import
            metrics["reactive_export_varh"] += reactive_export
            
            # Current power (positive = import, negative = export)
            if active_power > 0:
                metrics["import_power_w"] += active_power
            else:
                metrics["export_power_w"] += abs(active_power)
        
        # Set timestamp from meter data if available
        if data and len(data) > 0 and "timestamp" in data[0]:
            metrics["timestamp"] = data[0]["timestamp"]
        
        return metrics

    def get_monthly_aggregates(self, month=None, year=None):
        """Get monthly totals (cloud API only)"""
        if not self.use_cloud:
            print("Monthly aggregates require cloud API")
            return None
            
        end_date = datetime.now() if not (month and year) else datetime(year, month, 1)
        start_date = end_date.replace(day=1) - timedelta(days=1)
        start_date = start_date.replace(day=1)
        
        url = 'https://api.enphaseenergy.com/api/v4/systems/stats'
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'granularity': 'day',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        totals = defaultdict(float)
        for entry in data.get('stats', []):
            totals['grid_import'] += entry.get('grid_imported_energy', 0)
            totals['grid_export'] += entry.get('grid_exported_energy', 0)
            totals['battery_charge'] += entry.get('storage_charged_energy', 0)
            totals['battery_discharge'] += entry.get('storage_discharged_energy', 0)
        
        return totals

    def continuous_monitoring(self):
        """For local API: Collect and aggregate data over time"""
        if self.use_cloud:
            print("Use get_monthly_aggregates() for cloud data")
            return
            
        # Initialize persistent storage
        try:
            with open('enphase_stats.json', 'r') as f:
                stats = json.load(f)
        except FileNotFoundError:
            stats = defaultdict(dict)
            
        # Collect current data
        data = self.get_meter_data()
        if not data:
            return
            
        # Update hourly aggregates
        now = datetime.now()
        hour_key = now.strftime('%Y-%m-%d %H:00')
        
        for metric, value in data.items():
            if metric not in stats[hour_key]:
                stats[hour_key][metric] = 0
            stats[hour_key][metric] += value
        
        # Save updated stats
        with open('enphase_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)

# Example usage
if __name__ == "__main__":
    # Replace these with your actual credentials
    monitor = EnphaseEnergyMonitor(
        use_cloud=True,
        client_id="your-client-id",
        client_secret="your-client-secret",
        envoy_host="envoy.local",
        username="your-email@example.com",
        password="your-password",
        envoy_serial="000000000000",
        bearer_token="your-bearer-token",
        system_id="0000000"
    )
    
    # Cloud configuration
    # monitor = EnphaseEnergyMonitor(
    #     use_cloud=True,
    #     client_id='your_client_id',
    #     client_secret='your_client_secret'
    # )
    
    # Get current metrics
    print("Current Metrics:")
    print(monitor.get_meter_data())
    
    # Get monthly aggregates (cloud only)
    if monitor.use_cloud:
        print("\nMonthly Aggregates:")
        print(monitor.get_monthly_aggregates())
    
    # For local continuous monitoring
    monitor.continuous_monitoring()
