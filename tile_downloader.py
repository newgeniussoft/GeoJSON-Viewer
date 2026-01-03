"""
Offline Map Tile System
This script downloads map tiles and creates an offline map viewer.
"""

import os
import sys
import json
import requests
import time
from pathlib import Path
import folium
from folium import TileLayer

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, local
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QProgressBar, QTextEdit,
    QGroupBox, QGridLayout, QSpinBox, QMessageBox, QFileDialog, QCheckBox
)
from PyQt5.QtCore import QThread, pyqtSignal, Qt
from PyQt5.QtGui import QFont

class TileDownloader:
    """Download map tiles for offline use"""
    
    def __init__(self, tile_dir="map_tiles", max_workers=10, progress_callback=None):
        self.tile_dir = Path(tile_dir)
        self.tile_dir.mkdir(exist_ok=True)
        
        # OpenStreetMap tile server (free to use with attribution)
        self.tile_url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        
        # User agent (required by OSM tile server)
        self.headers = {
            'User-Agent': 'OfflineMapApp/1.0'
        }
        
        # Number of parallel download threads
        self.max_workers = max_workers
        
        # Thread-safe counter and lock
        self.lock = Lock()
        self.downloaded_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        
        # Progress callback for GUI updates
        self.progress_callback = progress_callback
        self.stop_requested = False

        self._thread_local = local()

    def _get_session(self):
        session = getattr(self._thread_local, 'session', None)
        if session is None:
            session = requests.Session()
            session.headers.update(self.headers)
            self._thread_local.session = session
        return session
    
    def lat_lon_to_tile(self, lat, lon, zoom):
        """Convert latitude/longitude to tile coordinates"""
        lat_rad = math.radians(lat)
        n = 2.0 ** zoom
        x = int((lon + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x, y
    
    def estimate_tiles_for_area(self, lat1, lon1, lat2, lon2, zoom_levels):
        """Return per-zoom tile counts and total for a lat/lon bounding box."""
        counts = []
        total = 0
        for zoom in zoom_levels:
            x1, y1 = self.lat_lon_to_tile(lat1, lon1, zoom)
            x2, y2 = self.lat_lon_to_tile(lat2, lon2, zoom)

            x_min, x_max = min(x1, x2), max(x1, x2)
            y_min, y_max = min(y1, y2), max(y1, y2)

            tiles = (x_max - x_min + 1) * (y_max - y_min + 1)
            counts.append((zoom, tiles))
            total += tiles
        return counts, total
    
    def download_tile(self, z, x, y, verbose=False):
        """Download a single tile"""
        if self.stop_requested:
            return 'stopped'
            
        tile_path = self.tile_dir / str(z) / str(x)

        tile_path.mkdir(parents=True, exist_ok=True)
        
        tile_file = tile_path / f"{y}.png"
        
        if tile_file.exists():
            with self.lock:
                self.skipped_count += 1
            if verbose:
                print(f"Tile {z}/{x}/{y} already exists, skipping")
            return 'skipped'
        
        url = self.tile_url.format(z=z, x=x, y=y)
        
        try:
            session = self._get_session()
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                with open(tile_file, 'wb') as f:
                    f.write(response.content)
                with self.lock:
                    self.downloaded_count += 1
                if verbose:
                    print(f"Downloaded tile {z}/{x}/{y}")
                return 'success'
            else:
                if verbose:
                    print(f"Failed to download {z}/{x}/{y}: {response.status_code}")
                with self.lock:
                    self.failed_count += 1
                return 'failed'
        except Exception as e:
            if verbose:
                print(f"Error downloading {z}/{x}/{y}: {e}")
            with self.lock:
                self.failed_count += 1
            return 'error'
    
    def stop(self):
        """Request download to stop"""
        self.stop_requested = True
    
    def download_area(self, lat1, lon1, lat2, lon2, zoom_levels, verbose=False):
        """
        Download tiles for a geographic area (parallel downloads)
        
        Args:
            lat1, lon1: Southwest corner
            lat2, lon2: Northeast corner
            zoom_levels: List of zoom levels to download (e.g., [10, 11, 12])
            verbose: Show detailed progress for each tile
        """
        self.stop_requested = False
        print(f"Starting parallel download for area ({lat1},{lon1}) to ({lat2},{lon2})")
        print(f"Using {self.max_workers} parallel workers\n")
        
        total_start_time = time.time()
        
        for zoom_idx, zoom in enumerate(zoom_levels):
            if self.stop_requested:
                print("\nDownload stopped by user")
                break
                
            x1, y1 = self.lat_lon_to_tile(lat1, lon1, zoom)
            x2, y2 = self.lat_lon_to_tile(lat2, lon2, zoom)
            
            # Ensure correct order
            x_min, x_max = min(x1, x2), max(x1, x2)
            y_min, y_max = min(y1, y2), max(y1, y2)
            
            total_tiles = (x_max - x_min + 1) * (y_max - y_min + 1)
            print(f"Zoom {zoom}: {total_tiles} tiles to process")
            
            # Reset counters
            self.downloaded_count = 0
            self.skipped_count = 0
            self.failed_count = 0
            
            # Download tiles in parallel (bounded in-flight tasks to keep RAM constant)
            zoom_start_time = time.time()

            max_in_flight = max(1, self.max_workers * 4)
            completed = 0

            def tile_iter():
                for x in range(x_min, x_max + 1):
                    if self.stop_requested:
                        return
                    for y in range(y_min, y_max + 1):
                        if self.stop_requested:
                            return
                        yield (zoom, x, y)

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                in_flight = set()

                for z, x, y in tile_iter():
                    if self.stop_requested:
                        break
                    in_flight.add(executor.submit(self.download_tile, z, x, y, verbose))

                    if len(in_flight) >= max_in_flight:
                        done = next(as_completed(in_flight))
                        in_flight.remove(done)
                        completed += 1

                        if not verbose and completed % 100 == 0:
                            progress = (completed / total_tiles) * 100
                            msg = (
                                f"  Progress: {completed}/{total_tiles} ({progress:.1f}%) - "
                                f"Downloaded: {self.downloaded_count}, Skipped: {self.skipped_count}, Failed: {self.failed_count}"
                            )
                            print(msg, end='\r')

                        if self.progress_callback and (completed % 25 == 0 or completed == total_tiles):
                            self.progress_callback(
                                zoom_idx, len(zoom_levels),
                                completed, total_tiles,
                                self.downloaded_count, self.skipped_count,
                                zoom
                            )

                for done in as_completed(in_flight):
                    if self.stop_requested:
                        break
                    completed += 1
                    if not verbose and completed % 100 == 0:
                        progress = (completed / total_tiles) * 100
                        msg = (
                            f"  Progress: {completed}/{total_tiles} ({progress:.1f}%) - "
                            f"Downloaded: {self.downloaded_count}, Skipped: {self.skipped_count}, Failed: {self.failed_count}"
                        )
                        print(msg, end='\r')

                    if self.progress_callback and (completed % 25 == 0 or completed == total_tiles):
                        self.progress_callback(
                            zoom_idx, len(zoom_levels),
                            completed, total_tiles,
                            self.downloaded_count, self.skipped_count,
                            zoom
                        )
            
            if self.stop_requested:
                break
                
            zoom_elapsed = time.time() - zoom_start_time
            print(f"\n  Zoom {zoom} complete in {zoom_elapsed:.1f}s: "
                  f"Downloaded: {self.downloaded_count}, Skipped: {self.skipped_count}, Failed: {self.failed_count}, "
                  f"Total: {total_tiles}\n")
        
        total_elapsed = time.time() - total_start_time
        print(f"\n=== Download Complete ===")
        print(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")


class OfflineMapViewer:
    """Create an offline map viewer using downloaded tiles"""
    
    def __init__(self, tile_dir="map_tiles"):
        self.tile_dir = Path(tile_dir).absolute()
    
    def create_map(self, center_lat, center_lon, zoom=13, output_file="offline_map.html"):
        """
        Create an offline map HTML file
        
        Args:
            center_lat, center_lon: Center coordinates
            zoom: Initial zoom level
            output_file: Output HTML filename
        """
        # Create the map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom,
            tiles=None  # Don't use default tiles
        )
        
        # Add offline tile layer
        # Use file:// protocol to load local tiles
        tile_path = f"file:///{self.tile_dir}" + "/{z}/{x}/{y}.png"
        
        TileLayer(
            tiles=tile_path,
            attr='OpenStreetMap',
            name='Offline Map',
            overlay=False,
            control=True
        ).add_to(m)
        
        # Add a marker at the center
        folium.Marker(
            [center_lat, center_lon],
            popup='Center Point',
            tooltip='You are here'
        ).add_to(m)
        
        # Save the map
        m.save(output_file)
        print(f"Map saved to {output_file}")
        print(f"Open it in a browser to view your offline map!")
        
        return m


class DownloadThread(QThread):
    """Background thread for downloading tiles"""
    progress = pyqtSignal(int, int, int, int, int, int, int)  # zoom_idx, total_zooms, completed, total, downloaded, skipped, zoom
    finished = pyqtSignal(str)
    error = pyqtSignal(str)
    
    def __init__(self, lat1, lon1, lat2, lon2, zoom_levels, tile_dir, max_workers):
        super().__init__()
        self.lat1 = lat1
        self.lon1 = lon1
        self.lat2 = lat2
        self.lon2 = lon2
        self.zoom_levels = zoom_levels
        self.tile_dir = tile_dir
        self.max_workers = max_workers
        self.downloader = None
    
    def run(self):
        try:
            self.downloader = TileDownloader(
                tile_dir=self.tile_dir,
                max_workers=self.max_workers,
                progress_callback=self.progress.emit
            )
            self.downloader.download_area(
                self.lat1, self.lon1, self.lat2, self.lon2,
                self.zoom_levels
            )
            self.finished.emit("Download completed successfully!")
        except Exception as e:
            self.error.emit(f"Download error: {str(e)}")
    
    def stop(self):
        if self.downloader:
            self.downloader.stop()


class TileDownloaderGUI(QMainWindow):
    """PyQt5 GUI for tile downloader"""
    
    def __init__(self):
        super().__init__()
        self.download_thread = None
        self.geojson_bbox = None
        self.geojson_path = ''
        self.init_ui()
    
    def init_ui(self):
        self.setWindowTitle('Offline Map Tile Downloader')
        self.setGeometry(100, 100, 800, 700)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Title
        title = QLabel('Offline Map Tile Downloader')
        title.setFont(QFont('Arial', 16, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title)
        
        # Coordinates group
        coords_group = QGroupBox('Download Area Coordinates (Manual)')
        coords_layout = QGridLayout()
        
        coords_layout.addWidget(QLabel('Southwest Latitude:'), 0, 0)
        self.lat1_input = QLineEdit('-25.32070100805562')
        coords_layout.addWidget(self.lat1_input, 0, 1)
        
        coords_layout.addWidget(QLabel('Southwest Longitude:'), 1, 0)
        self.lon1_input = QLineEdit('41.96438296337088')
        coords_layout.addWidget(self.lon1_input, 1, 1)
        
        coords_layout.addWidget(QLabel('Northeast Latitude:'), 2, 0)
        self.lat2_input = QLineEdit('-11.44084669828657')
        coords_layout.addWidget(self.lat2_input, 2, 1)
        
        coords_layout.addWidget(QLabel('Northeast Longitude:'), 3, 0)
        self.lon2_input = QLineEdit('53.485718448728164')
        coords_layout.addWidget(self.lon2_input, 3, 1)
        
        coords_group.setLayout(coords_layout)
        main_layout.addWidget(coords_group)

        # Extent mode group
        extent_group = QGroupBox('Extent Source')
        extent_layout = QGridLayout()

        self.use_geojson_checkbox = QCheckBox('Use GeoJSON extent (recommended for high zoom)')
        self.use_geojson_checkbox.stateChanged.connect(self.on_extent_mode_changed)
        extent_layout.addWidget(self.use_geojson_checkbox, 0, 0, 1, 3)

        extent_layout.addWidget(QLabel('GeoJSON File:'), 1, 0)
        self.geojson_path_input = QLineEdit('')
        self.geojson_path_input.setPlaceholderText('Select a GeoJSON file to compute extent')
        extent_layout.addWidget(self.geojson_path_input, 1, 1)
        self.geojson_browse_btn = QPushButton('Browse')
        self.geojson_browse_btn.clicked.connect(self.browse_geojson)
        extent_layout.addWidget(self.geojson_browse_btn, 1, 2)

        extent_layout.addWidget(QLabel('Buffer (meters):'), 2, 0)
        self.buffer_m_spin = QSpinBox()
        self.buffer_m_spin.setRange(0, 200000)
        self.buffer_m_spin.setValue(500)
        self.buffer_m_spin.valueChanged.connect(self.recompute_geojson_bbox)
        extent_layout.addWidget(self.buffer_m_spin, 2, 1)

        self.extent_info_label = QLabel('Extent: -')
        self.extent_info_label.setStyleSheet('color: #555;')
        extent_layout.addWidget(self.extent_info_label, 3, 0, 1, 3)

        extent_group.setLayout(extent_layout)
        main_layout.addWidget(extent_group)
        
        # Settings group
        settings_group = QGroupBox('Download Settings')
        settings_layout = QGridLayout()
        
        settings_layout.addWidget(QLabel('Zoom Start:'), 0, 0)
        self.zoom_start = QSpinBox()
        self.zoom_start.setRange(0, 19)
        self.zoom_start.setValue(6)
        settings_layout.addWidget(self.zoom_start, 0, 1)
        
        settings_layout.addWidget(QLabel('Zoom End:'), 1, 0)
        self.zoom_end = QSpinBox()
        self.zoom_end.setRange(0, 19)
        self.zoom_end.setValue(14)
        settings_layout.addWidget(self.zoom_end, 1, 1)
        
        settings_layout.addWidget(QLabel('Parallel Workers:'), 2, 0)
        self.workers_input = QSpinBox()
        self.workers_input.setRange(1, 50)
        self.workers_input.setValue(10)
        settings_layout.addWidget(self.workers_input, 2, 1)
        
        settings_layout.addWidget(QLabel('Tile Directory:'), 3, 0)
        self.tile_dir_input = QLineEdit('map_tiles')
        settings_layout.addWidget(self.tile_dir_input, 3, 1)
        
        settings_group.setLayout(settings_layout)
        main_layout.addWidget(settings_group)
        
        # Progress group
        progress_group = QGroupBox('Download Progress')
        progress_layout = QVBoxLayout()
        
        # Overall progress
        progress_layout.addWidget(QLabel('Overall Progress:'))
        self.overall_progress = QProgressBar()
        progress_layout.addWidget(self.overall_progress)
        
        # Current zoom progress
        self.zoom_label = QLabel('Current Zoom: -')
        progress_layout.addWidget(self.zoom_label)
        self.zoom_progress = QProgressBar()
        progress_layout.addWidget(self.zoom_progress)
        
        # Statistics
        self.stats_label = QLabel('Downloaded: 0 | Skipped: 0 | Total: 0')
        progress_layout.addWidget(self.stats_label)
        
        progress_group.setLayout(progress_layout)
        main_layout.addWidget(progress_group)
        
        # Log output
        log_group = QGroupBox('Log')
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(150)
        log_layout.addWidget(self.log_output)
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.start_btn = QPushButton('Start Download')
        self.start_btn.clicked.connect(self.start_download)
        self.start_btn.setStyleSheet('background-color: #4CAF50; color: white; font-weight: bold; padding: 10px;')
        button_layout.addWidget(self.start_btn)
        
        self.stop_btn = QPushButton('Stop')
        self.stop_btn.clicked.connect(self.stop_download)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setStyleSheet('background-color: #f44336; color: white; font-weight: bold; padding: 10px;')
        button_layout.addWidget(self.stop_btn)
        
        main_layout.addLayout(button_layout)
        
        # Status bar
        self.statusBar().showMessage('Ready to download tiles')
    
    def log(self, message):
        """Add message to log output"""
        self.log_output.append(message)

    def on_extent_mode_changed(self):
        use_geojson = self.use_geojson_checkbox.isChecked()
        for w in (self.lat1_input, self.lon1_input, self.lat2_input, self.lon2_input):
            w.setEnabled(not use_geojson)

        self.geojson_path_input.setEnabled(use_geojson)
        self.geojson_browse_btn.setEnabled(use_geojson)
        self.buffer_m_spin.setEnabled(use_geojson)
        if use_geojson:
            self.recompute_geojson_bbox()
        else:
            self.extent_info_label.setText('Extent: -')

    def browse_geojson(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            'Select GeoJSON file',
            '',
            'GeoJSON Files (*.json *.geojson);;All Files (*)'
        )
        if not file_path:
            return
        self.geojson_path_input.setText(file_path)
        self.recompute_geojson_bbox()

    def recompute_geojson_bbox(self):
        if not self.use_geojson_checkbox.isChecked():
            return

        path = self.geojson_path_input.text().strip()
        if not path or not os.path.exists(path):
            self.geojson_bbox = None
            self.extent_info_label.setText('Extent: GeoJSON not selected')
            return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            features = data.get('features', []) if isinstance(data, dict) else []
            if not features:
                self.geojson_bbox = None
                self.extent_info_label.setText('Extent: No features found in GeoJSON')
                return

            min_lon = float('inf')
            min_lat = float('inf')
            max_lon = float('-inf')
            max_lat = float('-inf')

            def scan_coords(coords):
                nonlocal min_lon, min_lat, max_lon, max_lat
                if not coords:
                    return
                if isinstance(coords[0], (float, int)) and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]
                    min_lon = min(min_lon, lon)
                    max_lon = max(max_lon, lon)
                    min_lat = min(min_lat, lat)
                    max_lat = max(max_lat, lat)
                else:
                    for c in coords:
                        scan_coords(c)

            for feat in features:
                geom = feat.get('geometry') or {}
                coords = geom.get('coordinates')
                if coords is not None:
                    scan_coords(coords)

            if not math.isfinite(min_lon) or not math.isfinite(min_lat):
                self.geojson_bbox = None
                self.extent_info_label.setText('Extent: Could not compute bbox from GeoJSON')
                return

            # Apply buffer in meters (approx)
            buffer_m = float(self.buffer_m_spin.value())
            lat_center = (min_lat + max_lat) / 2.0
            lat_deg = buffer_m / 111320.0
            lon_deg = buffer_m / (111320.0 * max(0.1, math.cos(math.radians(lat_center))))

            min_lat_b = min_lat - lat_deg
            max_lat_b = max_lat + lat_deg
            min_lon_b = min_lon - lon_deg
            max_lon_b = max_lon + lon_deg

            self.geojson_bbox = (min_lat_b, min_lon_b, max_lat_b, max_lon_b)
            self.geojson_path = path
            self.extent_info_label.setText(
                f'Extent: ({min_lat_b:.6f},{min_lon_b:.6f}) to ({max_lat_b:.6f},{max_lon_b:.6f})'
            )
        except Exception as e:
            self.geojson_bbox = None
            self.extent_info_label.setText(f'Extent: Error reading GeoJSON ({e})')

    def confirm_estimate(self, lat1, lon1, lat2, lon2, zoom_levels, tile_dir):
        downloader = TileDownloader(tile_dir=tile_dir, max_workers=1)
        per_zoom, total_tiles = downloader.estimate_tiles_for_area(lat1, lon1, lat2, lon2, zoom_levels)

        # very rough average tile size; depends on area and style
        avg_kb = 20
        est_gb = (total_tiles * avg_kb) / (1024 * 1024)

        lines = []
        lines.append(f'Total tiles: {total_tiles:,}')
        lines.append(f'Estimated disk: ~{est_gb:.2f} GB (assuming {avg_kb} KB/tile)')
        lines.append(f'Directory: {os.path.abspath(tile_dir)}')
        lines.append('')
        lines.append('Per zoom:')
        for z, cnt in per_zoom:
            lines.append(f'  z{z}: {cnt:,} tiles')

        msg = '\n'.join(lines)
        # If this is huge, force explicit confirmation
        title = 'Confirm Download'
        return QMessageBox.question(
            self,
            title,
            msg + '\n\nContinue?',
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        ) == QMessageBox.Yes
    
    def start_download(self):
        """Start the download process"""
        try:
            # Parse inputs
            zoom_start = self.zoom_start.value()
            zoom_end = self.zoom_end.value()
            max_workers = self.workers_input.value()
            tile_dir = self.tile_dir_input.text()
            
            if zoom_start > zoom_end:
                QMessageBox.warning(self, 'Invalid Range', 'Zoom start must be <= zoom end')
                return
            
            zoom_levels = list(range(zoom_start, zoom_end + 1))

            # Determine extent (manual or GeoJSON)
            if self.use_geojson_checkbox.isChecked():
                self.recompute_geojson_bbox()
                if not self.geojson_bbox:
                    QMessageBox.warning(self, 'GeoJSON Extent', 'Please select a valid GeoJSON file to compute extent.')
                    return
                lat1, lon1, lat2, lon2 = self.geojson_bbox
            else:
                lat1 = float(self.lat1_input.text())
                lon1 = float(self.lon1_input.text())
                lat2 = float(self.lat2_input.text())
                lon2 = float(self.lon2_input.text())

            # Safety estimate confirmation
            if not self.confirm_estimate(lat1, lon1, lat2, lon2, zoom_levels, tile_dir):
                return
            
            # Reset UI
            self.overall_progress.setValue(0)
            self.zoom_progress.setValue(0)
            self.log_output.clear()
            self.log(f'Starting download for zoom levels {zoom_start}-{zoom_end}')
            self.log(f'Area: ({lat1},{lon1}) to ({lat2},{lon2})')
            self.log(f'Using {max_workers} parallel workers\n')
            
            # Disable start, enable stop
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            
            # Start download thread
            self.download_thread = DownloadThread(
                lat1, lon1, lat2, lon2, zoom_levels, tile_dir, max_workers
            )
            self.download_thread.progress.connect(self.update_progress)
            self.download_thread.finished.connect(self.download_finished)
            self.download_thread.error.connect(self.download_error)
            self.download_thread.start()
            
            self.statusBar().showMessage('Downloading tiles...')
        
        except ValueError:
            QMessageBox.critical(self, 'Input Error', 'Please enter valid numeric coordinates')
    
    def stop_download(self):
        """Stop the download process"""
        if self.download_thread:
            self.log('\nStopping download...')
            self.download_thread.stop()
            self.statusBar().showMessage('Stopping download...')
    
    def update_progress(self, zoom_idx, total_zooms, completed, total, downloaded, skipped, zoom):
        """Update progress bars and statistics"""
        # Overall progress
        overall_fraction = (zoom_idx + (completed / total if total else 0.0)) / total_zooms if total_zooms else 0.0
        overall_percent = int(overall_fraction * 100)
        self.overall_progress.setValue(overall_percent)
        
        # Zoom progress
        zoom_percent = int((completed / total) * 100) if total > 0 else 0
        self.zoom_progress.setValue(zoom_percent)
        
        # Update labels
        self.zoom_label.setText(f'Current Zoom: {zoom} ({zoom_idx + 1}/{total_zooms})')
        self.stats_label.setText(
            f'Downloaded: {downloaded} | Skipped: {skipped} | '
            f'Progress: {completed}/{total} ({zoom_percent}%)'
        )
    
    def download_finished(self, message):
        """Handle download completion"""
        self.log(f'\n{message}')
        self.statusBar().showMessage(message)
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.overall_progress.setValue(100)
        QMessageBox.information(self, 'Complete', message)
    
    def download_error(self, error_msg):
        """Handle download error"""
        self.log(f'\nERROR: {error_msg}')
        self.statusBar().showMessage('Download failed')
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        QMessageBox.critical(self, 'Error', error_msg)


# Example usage
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    gui = TileDownloaderGUI()
    gui.show()
    
    sys.exit(app.exec_())