#!/usr/bin/env python3
"""
GeoJSON Viewer - A PyQt5 application to display GeoJSON data in a table format
Optimized for handling large datasets with pagination and lazy loading
"""

import sys
import json
import tempfile
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem, 
    QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QLabel, 
    QFileDialog, QLineEdit, QMessageBox, QHeaderView, QProgressBar,
    QSpinBox, QComboBox, QDialog, QDialogButtonBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QUrl
from PyQt5.QtGui import QFont, QKeySequence
import os
try:
    from shapely.geometry import shape, mapping
    from shapely.ops import unary_union
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False

try:
    import folium
    from PyQt5.QtWebEngineWidgets import QWebEngineView
    FOLIUM_AVAILABLE = True
except ImportError:
    FOLIUM_AVAILABLE = False


class GeoJSONLoader(QThread):
    """Background thread for loading GeoJSON files without blocking UI"""
    progress = pyqtSignal(int)
    finished = pyqtSignal(list, list)
    error = pyqtSignal(str)
    
    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path
        
    def run(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract features from GeoJSON
            if 'features' in data:
                features = data['features']
            elif isinstance(data, list):
                features = data
            else:
                features = [data]
            
            # Extract all unique keys from properties
            all_keys = set()
            for idx, feature in enumerate(features):
                if idx % 100 == 0:
                    progress_percent = int((idx / len(features)) * 50)
                    self.progress.emit(progress_percent)
                
                if isinstance(feature, dict):
                    if 'properties' in feature:
                        all_keys.update(feature['properties'].keys())
                    else:
                        all_keys.update(feature.keys())
            
            # Sort keys for consistent column order
            sorted_keys = sorted(list(all_keys))
            
            # Extract data rows
            rows = []
            for idx, feature in enumerate(features):
                if idx % 100 == 0:
                    progress_percent = 50 + int((idx / len(features)) * 50)
                    self.progress.emit(progress_percent)
                
                row = {}
                if isinstance(feature, dict):
                    if 'properties' in feature:
                        row = feature['properties']
                    else:
                        row = feature
                rows.append(row)
            
            self.progress.emit(100)
            self.finished.emit(sorted_keys, rows)
            
        except Exception as e:
            self.error.emit(str(e))


class GeoJSONSaver(QThread):
    """Background thread for saving GeoJSON files without blocking UI"""
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)
    
    def __init__(self, file_path, original_data, updated_data):
        super().__init__()
        self.file_path = file_path
        self.original_data = original_data
        self.updated_data = updated_data
        
    def run(self):
        try:
            self.progress.emit(10, 'Preparing data...')
            
            # Update the properties in features
            if 'features' in self.original_data:
                total = len(self.original_data['features'])
                for i, feature in enumerate(self.original_data['features']):
                    if i < len(self.updated_data):
                        if 'properties' in feature:
                            feature['properties'] = self.updated_data[i]
                        else:
                            self.original_data['features'][i] = self.updated_data[i]
                    
                    if i % 100 == 0:
                        progress_percent = 10 + int((i / total) * 40)
                        self.progress.emit(progress_percent, f'Processing {i}/{total} records...')
            elif isinstance(self.original_data, list):
                self.original_data = self.updated_data
            else:
                self.original_data = self.updated_data[0] if self.updated_data else {}
            
            self.progress.emit(60, 'Writing to file...')
            
            # Save to file
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(self.original_data, f, ensure_ascii=False, indent=2)
            
            self.progress.emit(100, 'Save complete!')
            self.finished.emit(os.path.basename(self.file_path))
            
        except Exception as e:
            self.error.emit(str(e))


class GeoJSONViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.all_data = []
        self.all_keys = []
        self.current_page = 0
        self.rows_per_page = 100
        self.filtered_data = []
        self.search_text = ""
        self.current_file_path = None
        self.data_modified = False
        self.sort_column = -1
        self.sort_order = Qt.AscendingOrder
        self.current_matches = []
        self.current_match_index = -1
        self.original_geojson = None
        
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle('GeoJSON Viewer')
        self.setGeometry(100, 100, 1200, 700)
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # Top controls layout
        top_layout = QHBoxLayout()
        
        # File selection
        self.file_label = QLabel('No file loaded')
        self.file_label.setStyleSheet("font-weight: bold;")
        top_layout.addWidget(self.file_label)
        
        load_btn = QPushButton('Load GeoJSON File')
        load_btn.clicked.connect(self.load_file)
        top_layout.addWidget(load_btn)
        
        self.save_btn = QPushButton('Save Changes')
        self.save_btn.clicked.connect(self.save_file)
        self.save_btn.setEnabled(False)
        self.save_btn.setStyleSheet("font-weight: bold; color: green;")
        top_layout.addWidget(self.save_btn)
        
        self.merge_btn = QPushButton('Merge Polygons')
        self.merge_btn.clicked.connect(self.merge_polygons_dialog)
        self.merge_btn.setEnabled(False)
        self.merge_btn.setStyleSheet("font-weight: bold; color: blue;")
        self.merge_btn.setToolTip('Union child polygons by parent attribute')
        top_layout.addWidget(self.merge_btn)
        
        self.map_btn = QPushButton('View Map')
        self.map_btn.clicked.connect(self.show_map)
        self.map_btn.setEnabled(False)
        self.map_btn.setStyleSheet("font-weight: bold; color: purple;")
        self.map_btn.setToolTip('Display polygons on interactive map')
        top_layout.addWidget(self.map_btn)
        
        self.save_status_label = QLabel('')
        self.save_status_label.setStyleSheet("color: blue; font-style: italic;")
        top_layout.addWidget(self.save_status_label)
        
        top_layout.addStretch()
        
        main_layout.addLayout(top_layout)
        
        # Progress bar for loading
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # Progress bar for saving
        self.save_progress_bar = QProgressBar()
        self.save_progress_bar.setVisible(False)
        self.save_progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #4CAF50; }")
        main_layout.addWidget(self.save_progress_bar)
        
        # Search and filter layout
        filter_layout = QVBoxLayout()
        
        # First row: Search
        search_row = QHBoxLayout()
        search_row.addWidget(QLabel('Find:'))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText('Type to search in all columns...')
        self.search_input.textChanged.connect(self.on_search)
        self.search_input.returnPressed.connect(self.find_next)
        search_row.addWidget(self.search_input)
        
        self.match_label = QLabel('0 matches')
        self.match_label.setStyleSheet('color: gray;')
        search_row.addWidget(self.match_label)
        
        find_prev_btn = QPushButton('◀ Prev')
        find_prev_btn.clicked.connect(self.find_previous)
        find_prev_btn.setMaximumWidth(80)
        search_row.addWidget(find_prev_btn)
        
        find_next_btn = QPushButton('Next ▶')
        find_next_btn.clicked.connect(self.find_next)
        find_next_btn.setMaximumWidth(80)
        search_row.addWidget(find_next_btn)
        
        clear_search_btn = QPushButton('Clear')
        clear_search_btn.clicked.connect(self.clear_search)
        clear_search_btn.setMaximumWidth(60)
        search_row.addWidget(clear_search_btn)
        
        filter_layout.addLayout(search_row)
        
        # Second row: Replace
        replace_row = QHBoxLayout()
        replace_row.addWidget(QLabel('Replace:'))
        self.replace_input = QLineEdit()
        self.replace_input.setPlaceholderText('Replace with...')
        self.replace_input.returnPressed.connect(self.replace_current)
        replace_row.addWidget(self.replace_input)
        
        replace_row.addWidget(QLabel(''))  # Spacer for alignment
        
        replace_btn = QPushButton('Replace')
        replace_btn.clicked.connect(self.replace_current)
        replace_btn.setMaximumWidth(80)
        replace_row.addWidget(replace_btn)
        
        replace_all_btn = QPushButton('Replace All')
        replace_all_btn.clicked.connect(self.replace_all)
        replace_all_btn.setMaximumWidth(80)
        replace_all_btn.setStyleSheet('font-weight: bold;')
        replace_row.addWidget(replace_all_btn)
        
        replace_row.addWidget(QLabel(''))  # Spacer for alignment
        
        filter_layout.addLayout(replace_row)
        
        main_layout.addLayout(filter_layout)
        
        # Table widget
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setEditTriggers(QTableWidget.DoubleClicked | QTableWidget.EditKeyPressed)
        self.table.setSelectionBehavior(QTableWidget.SelectItems)
        self.table.setSortingEnabled(False)
        self.table.horizontalHeader().sectionClicked.connect(self.on_header_clicked)
        self.table.itemChanged.connect(self.on_item_changed)
        
        # Install event filter for paste functionality
        self.table.installEventFilter(self)
        
        main_layout.addWidget(self.table)
        
        # Pagination controls
        pagination_layout = QHBoxLayout()
        
        self.info_label = QLabel('No data loaded')
        pagination_layout.addWidget(self.info_label)
        
        pagination_layout.addStretch()
        
        pagination_layout.addWidget(QLabel('Rows per page:'))
        self.rows_per_page_combo = QComboBox()
        self.rows_per_page_combo.addItems(['50', '100', '200', '500', '1000'])
        self.rows_per_page_combo.setCurrentText('100')
        self.rows_per_page_combo.currentTextChanged.connect(self.on_rows_per_page_changed)
        pagination_layout.addWidget(self.rows_per_page_combo)
        
        self.first_btn = QPushButton('First')
        self.first_btn.clicked.connect(self.first_page)
        self.first_btn.setEnabled(False)
        pagination_layout.addWidget(self.first_btn)
        
        self.prev_btn = QPushButton('Previous')
        self.prev_btn.clicked.connect(self.prev_page)
        self.prev_btn.setEnabled(False)
        pagination_layout.addWidget(self.prev_btn)
        
        self.page_label = QLabel('Page 0 of 0')
        pagination_layout.addWidget(self.page_label)
        
        self.next_btn = QPushButton('Next')
        self.next_btn.clicked.connect(self.next_page)
        self.next_btn.setEnabled(False)
        pagination_layout.addWidget(self.next_btn)
        
        self.last_btn = QPushButton('Last')
        self.last_btn.clicked.connect(self.last_page)
        self.last_btn.setEnabled(False)
        pagination_layout.addWidget(self.last_btn)
        
        main_layout.addLayout(pagination_layout)
        
        # Status bar
        self.statusBar().showMessage('Ready')
        
    def load_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            'Open GeoJSON File',
            '',
            'GeoJSON Files (*.json *.geojson);;All Files (*.*)'
        )
        
        if file_path:
            # Check if there are unsaved changes
            if self.data_modified:
                reply = QMessageBox.question(
                    self,
                    'Unsaved Changes',
                    'You have unsaved changes. Do you want to save before loading a new file?',
                    QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel
                )
                
                if reply == QMessageBox.Cancel:
                    return
                elif reply == QMessageBox.Yes:
                    self.save_file()
            
            self.current_file_path = file_path
            self.file_label.setText(f'Loading: {os.path.basename(file_path)}')
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.statusBar().showMessage('Loading file...')
            
            # Store original GeoJSON for geometry operations
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    self.original_geojson = json.load(f)
            except:
                self.original_geojson = None
            
            # Load in background thread
            self.loader = GeoJSONLoader(file_path)
            self.loader.progress.connect(self.on_load_progress)
            self.loader.finished.connect(self.on_load_finished)
            self.loader.error.connect(self.on_load_error)
            self.loader.start()
            
    def on_load_progress(self, value):
        self.progress_bar.setValue(value)
        
    def on_load_finished(self, keys, data):
        self.all_keys = keys
        self.all_data = data
        self.filtered_data = data
        self.current_page = 0
        self.data_modified = False
        self.sort_column = -1
        self.sort_order = Qt.AscendingOrder
        
        self.file_label.setText(f'Loaded: {len(data)} records with {len(keys)} columns')
        self.progress_bar.setVisible(False)
        self.statusBar().showMessage(f'Successfully loaded {len(data)} records')
        self.save_btn.setEnabled(False)
        
        # Enable merge button if shapely is available and we have geometry
        if SHAPELY_AVAILABLE and self.original_geojson:
            self.merge_btn.setEnabled(True)
        
        # Enable map button if folium is available and we have geometry
        if FOLIUM_AVAILABLE and self.original_geojson:
            self.map_btn.setEnabled(True)
        
        self.display_page()
        self.update_pagination_controls()
        
    def on_load_error(self, error_msg):
        self.progress_bar.setVisible(False)
        self.file_label.setText('Error loading file')
        self.statusBar().showMessage('Error')
        QMessageBox.critical(self, 'Error', f'Failed to load file:\n{error_msg}')
        
    def display_page(self):
        if not self.filtered_data:
            self.table.setRowCount(0)
            self.table.setColumnCount(0)
            return
        
        # Block signals to prevent triggering itemChanged during population
        self.table.blockSignals(True)
        
        # Calculate page boundaries
        start_idx = self.current_page * self.rows_per_page
        end_idx = min(start_idx + self.rows_per_page, len(self.filtered_data))
        page_data = self.filtered_data[start_idx:end_idx]
        
        # Set up table
        self.table.setRowCount(len(page_data))
        self.table.setColumnCount(len(self.all_keys))
        self.table.setHorizontalHeaderLabels(self.all_keys)
        
        # Populate table
        for row_idx, row_data in enumerate(page_data):
            for col_idx, key in enumerate(self.all_keys):
                value = row_data.get(key, '')
                
                # Convert value to string
                if value is None:
                    value_str = 'NA'
                elif isinstance(value, (dict, list)):
                    value_str = json.dumps(value, ensure_ascii=False)
                else:
                    value_str = str(value)
                
                item = QTableWidgetItem(value_str)
                # Store the actual row index in filtered_data
                item.setData(Qt.UserRole, start_idx + row_idx)
                self.table.setItem(row_idx, col_idx, item)
        
        # Auto-resize columns to content
        self.table.resizeColumnsToContents()
        
        # Unblock signals
        self.table.blockSignals(False)
        
        # Update info
        total_pages = (len(self.filtered_data) + self.rows_per_page - 1) // self.rows_per_page
        self.page_label.setText(f'Page {self.current_page + 1} of {total_pages}')
        status_text = f'Showing {start_idx + 1}-{end_idx} of {len(self.filtered_data)} records'
        if len(self.filtered_data) != len(self.all_data):
            status_text += f' (filtered from {len(self.all_data)})'
        if self.data_modified:
            status_text += ' - MODIFIED'
        self.info_label.setText(status_text)
        
    def update_pagination_controls(self):
        total_pages = (len(self.filtered_data) + self.rows_per_page - 1) // self.rows_per_page
        
        self.first_btn.setEnabled(self.current_page > 0)
        self.prev_btn.setEnabled(self.current_page > 0)
        self.next_btn.setEnabled(self.current_page < total_pages - 1)
        self.last_btn.setEnabled(self.current_page < total_pages - 1)
        
    def first_page(self):
        self.current_page = 0
        self.display_page()
        self.update_pagination_controls()
        
    def prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.display_page()
            self.update_pagination_controls()
            
    def next_page(self):
        total_pages = (len(self.filtered_data) + self.rows_per_page - 1) // self.rows_per_page
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.display_page()
            self.update_pagination_controls()
            
    def last_page(self):
        total_pages = (len(self.filtered_data) + self.rows_per_page - 1) // self.rows_per_page
        self.current_page = max(0, total_pages - 1)
        self.display_page()
        self.update_pagination_controls()
        
    def on_rows_per_page_changed(self, value):
        self.rows_per_page = int(value)
        self.current_page = 0
        self.display_page()
        self.update_pagination_controls()
        
    def on_search(self, text):
        self.search_text = text.lower()
        
        if not self.search_text:
            self.filtered_data = self.all_data
            self.current_matches = []
            self.current_match_index = -1
            self.match_label.setText('0 matches')
            self.match_label.setStyleSheet('color: gray;')
        else:
            # Filter data based on search text and find all matches
            self.filtered_data = []
            self.current_matches = []
            
            for row_idx, row in enumerate(self.all_data):
                row_has_match = False
                # Search in all values
                for col_key, value in row.items():
                    value_str = str(value).lower()
                    if self.search_text in value_str:
                        if not row_has_match:
                            self.filtered_data.append(row)
                            row_has_match = True
                        # Store match location (row_idx, col_key)
                        self.current_matches.append((row_idx, col_key))
            
            self.current_match_index = -1
            match_count = len(self.current_matches)
            self.match_label.setText(f'{match_count} match{"es" if match_count != 1 else ""}')
            self.match_label.setStyleSheet('color: green;' if match_count > 0 else 'color: red;')
        
        self.current_page = 0
        self.display_page()
        self.update_pagination_controls()
        
    def clear_search(self):
        self.search_input.clear()
        self.replace_input.clear()
        self.current_matches = []
        self.current_match_index = -1
        self.match_label.setText('0 matches')
        self.match_label.setStyleSheet('color: gray;')
    
    def eventFilter(self, source, event):
        """Handle keyboard events for paste functionality"""
        if source == self.table and event.type() == event.KeyPress:
            if event.matches(QKeySequence.Paste):
                self.paste_from_clipboard()
                return True
        return super().eventFilter(source, event)
    
    def paste_from_clipboard(self):
        """Paste clipboard content into selected cells"""
        if not self.all_data:
            return
        
        # Get clipboard content
        clipboard = QApplication.clipboard()
        text = clipboard.text()
        
        if not text:
            return
        
        # Get selected cells
        selected_ranges = self.table.selectedRanges()
        selected_items = self.table.selectedItems()
        
        # Parse clipboard data (handle Excel-style tab/newline separated data)
        rows = text.split('\n')
        # Remove empty last row if exists
        if rows and not rows[-1].strip():
            rows.pop()
        
        paste_data = []
        for row in rows:
            # Split by tab for Excel-style paste, or use single value
            if '\t' in row:
                paste_data.append(row.split('\t'))
            else:
                paste_data.append([row])
        
        if not paste_data:
            return
        
        # Check if clipboard contains single value and multiple cells are selected
        is_single_value = len(paste_data) == 1 and len(paste_data[0]) == 1
        
        # Block signals during paste to prevent multiple itemChanged events
        self.table.blockSignals(True)
        
        modified_count = 0
        
        if is_single_value and len(selected_items) > 1:
            # Paste single value into all selected cells
            single_value = paste_data[0][0].strip()
            
            for item in selected_items:
                if not item:
                    continue
                
                # Get the actual data row index
                data_row_idx = item.data(Qt.UserRole)
                if data_row_idx is None:
                    continue
                
                # Get column key
                col_key = self.all_keys[item.column()]
                
                # Update the data
                old_value = self.filtered_data[data_row_idx].get(col_key, '')
                new_value = single_value
                
                # Try to convert to original type
                if isinstance(old_value, (int, float)):
                    try:
                        new_value = type(old_value)(new_value)
                    except ValueError:
                        pass
                elif new_value == 'NA':
                    new_value = None
                
                # Update both filtered_data and all_data
                self.filtered_data[data_row_idx][col_key] = new_value
                
                # Find and update in all_data
                for i, row in enumerate(self.all_data):
                    if row is self.filtered_data[data_row_idx]:
                        self.all_data[i][col_key] = new_value
                        break
                
                # Update the table item
                item.setText(str(new_value) if new_value is not None else 'NA')
                modified_count += 1
        else:
            # Multi-cell paste or single cell paste
            if not selected_ranges:
                # If no selection, try current cell
                current_item = self.table.currentItem()
                if not current_item:
                    self.table.blockSignals(False)
                    return
                selected_row = current_item.row()
                selected_col = current_item.column()
            else:
                # Use the first selected range
                selected_range = selected_ranges[0]
                selected_row = selected_range.topRow()
                selected_col = selected_range.leftColumn()
            
            # Paste data into cells starting from selected position
            for row_offset, paste_row in enumerate(paste_data):
                table_row = selected_row + row_offset
                if table_row >= self.table.rowCount():
                    break
                
                for col_offset, paste_value in enumerate(paste_row):
                    table_col = selected_col + col_offset
                    if table_col >= self.table.columnCount():
                        break
                    
                    # Get the item
                    item = self.table.item(table_row, table_col)
                    if not item:
                        continue
                    
                    # Get the actual data row index
                    data_row_idx = item.data(Qt.UserRole)
                    if data_row_idx is None:
                        continue
                    
                    # Get column key
                    col_key = self.all_keys[table_col]
                    
                    # Update the data
                    old_value = self.filtered_data[data_row_idx].get(col_key, '')
                    new_value = paste_value.strip()
                    
                    # Try to convert to original type
                    if isinstance(old_value, (int, float)):
                        try:
                            new_value = type(old_value)(new_value)
                        except ValueError:
                            pass
                    elif new_value == 'NA':
                        new_value = None
                    
                    # Update both filtered_data and all_data
                    self.filtered_data[data_row_idx][col_key] = new_value
                    
                    # Find and update in all_data
                    for i, row in enumerate(self.all_data):
                        if row is self.filtered_data[data_row_idx]:
                            self.all_data[i][col_key] = new_value
                            break
                    
                    # Update the table item
                    item.setText(str(new_value) if new_value is not None else 'NA')
                    modified_count += 1
        
        # Unblock signals
        self.table.blockSignals(False)
        
        # Mark as modified if any changes were made
        if modified_count > 0:
            self.data_modified = True
            if not self.save_btn.isEnabled():
                self.save_btn.setEnabled(True)
            
            # Update info label
            start_idx = self.current_page * self.rows_per_page
            end_idx = min(start_idx + self.rows_per_page, len(self.filtered_data))
            status_text = f'Showing {start_idx + 1}-{end_idx} of {len(self.filtered_data)} records'
            if len(self.filtered_data) != len(self.all_data):
                status_text += f' (filtered from {len(self.all_data)})'
            if self.data_modified:
                status_text += ' - MODIFIED'
            self.info_label.setText(status_text)
            
            self.statusBar().showMessage(f'Pasted {modified_count} cell(s)')
    
    def find_next(self):
        """Navigate to next match"""
        if not self.current_matches:
            return
        
        self.current_match_index = (self.current_match_index + 1) % len(self.current_matches)
        self.highlight_current_match()
    
    def find_previous(self):
        """Navigate to previous match"""
        if not self.current_matches:
            return
        
        self.current_match_index = (self.current_match_index - 1) % len(self.current_matches)
        self.highlight_current_match()
    
    def highlight_current_match(self):
        """Highlight and scroll to current match"""
        if self.current_match_index < 0 or self.current_match_index >= len(self.current_matches):
            return
        
        row_idx, col_key = self.current_matches[self.current_match_index]
        
        # Update match label
        self.match_label.setText(f'{self.current_match_index + 1} of {len(self.current_matches)}')
        self.match_label.setStyleSheet('color: blue; font-weight: bold;')
        
        # Find the row in filtered_data
        filtered_row_idx = -1
        for i, row in enumerate(self.filtered_data):
            if row is self.all_data[row_idx]:
                filtered_row_idx = i
                break
        
        if filtered_row_idx == -1:
            return
        
        # Navigate to correct page
        target_page = filtered_row_idx // self.rows_per_page
        if target_page != self.current_page:
            self.current_page = target_page
            self.display_page()
            self.update_pagination_controls()
        
        # Find column index
        col_idx = self.all_keys.index(col_key) if col_key in self.all_keys else -1
        if col_idx == -1:
            return
        
        # Calculate row in current page
        row_in_page = filtered_row_idx % self.rows_per_page
        
        # Select and scroll to the cell
        self.table.setCurrentCell(row_in_page, col_idx)
        self.table.scrollToItem(self.table.item(row_in_page, col_idx))
    
    def replace_current(self):
        """Replace the current match"""
        if self.current_match_index < 0 or self.current_match_index >= len(self.current_matches):
            QMessageBox.information(self, 'No Match', 'No match selected. Use Find Next/Previous to select a match.')
            return
        
        if not self.replace_input.text():
            reply = QMessageBox.question(
                self,
                'Empty Replace',
                'Replace text is empty. Do you want to replace with empty string (delete)?',
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
        
        row_idx, col_key = self.current_matches[self.current_match_index]
        old_value = str(self.all_data[row_idx].get(col_key, ''))
        
        # Perform case-insensitive replacement
        lower_old = old_value.lower()
        search_lower = self.search_text.lower()
        pos = lower_old.find(search_lower)
        
        if pos != -1:
            # Replace preserving the original case position
            new_value = old_value[:pos] + self.replace_input.text() + old_value[pos + len(self.search_text):]
        else:
            new_value = old_value
        
        # Update the data in all_data (which is referenced by filtered_data)
        self.all_data[row_idx][col_key] = new_value
        
        # Mark as modified
        self.data_modified = True
        if not self.save_btn.isEnabled():
            self.save_btn.setEnabled(True)
        
        # Remove this match from the list
        self.current_matches.pop(self.current_match_index)
        
        # Update match count and navigate to next
        if self.current_matches:
            if self.current_match_index >= len(self.current_matches):
                self.current_match_index = 0
            self.match_label.setText(f'{len(self.current_matches)} matches')
            # Refresh display first
            self.display_page()
            # Then highlight next match
            self.highlight_current_match()
        else:
            self.current_match_index = -1
            self.match_label.setText('0 matches')
            self.match_label.setStyleSheet('color: gray;')
            # Refresh display
            self.display_page()
        
        self.statusBar().showMessage('Replaced 1 occurrence')
    
    def replace_all(self):
        """Replace all matches"""
        if not self.current_matches:
            QMessageBox.information(self, 'No Matches', 'No matches found to replace.')
            return
        
        if not self.replace_input.text():
            reply = QMessageBox.question(
                self,
                'Empty Replace',
                f'Replace text is empty. Do you want to replace all {len(self.current_matches)} matches with empty string (delete)?',
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
        
        # Confirm replace all
        reply = QMessageBox.question(
            self,
            'Confirm Replace All',
            f'Replace all {len(self.current_matches)} occurrences of "{self.search_text}" with "{self.replace_input.text()}"?',
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.No:
            return
        
        replace_count = 0
        replace_text = self.replace_input.text()
        search_lower = self.search_text.lower()
        
        # Process all matches with case-insensitive replacement
        for row_idx, col_key in self.current_matches:
            old_value = str(self.all_data[row_idx].get(col_key, ''))
            lower_old = old_value.lower()
            
            # Replace all occurrences in this cell (case-insensitive)
            new_value = old_value
            offset = 0
            while True:
                pos = lower_old.find(search_lower, offset)
                if pos == -1:
                    break
                # Replace this occurrence
                new_value = new_value[:pos] + replace_text + new_value[pos + len(self.search_text):]
                lower_old = new_value.lower()
                offset = pos + len(replace_text)
            
            if old_value != new_value:
                self.all_data[row_idx][col_key] = new_value
                replace_count += 1
        
        # Mark as modified
        if replace_count > 0:
            self.data_modified = True
            if not self.save_btn.isEnabled():
                self.save_btn.setEnabled(True)
        
        # Store current search query to re-run after refresh
        current_search = self.search_input.text()
        
        # Clear matches
        self.current_matches = []
        self.current_match_index = -1
        
        # Refresh display
        self.display_page()
        
        # Re-run search to update filtered data if search was active
        if current_search:
            self.search_input.setText('')  # Clear first
            self.search_input.setText(current_search)  # Re-apply search
        else:
            self.match_label.setText('0 matches')
            self.match_label.setStyleSheet('color: gray;')
        
        self.statusBar().showMessage(f'Replaced {replace_count} occurrences')
        QMessageBox.information(self, 'Replace Complete', f'Successfully replaced {replace_count} occurrences.')
    
    def on_header_clicked(self, logical_index):
        """Handle column header clicks for sorting"""
        if not self.filtered_data:
            return
        
        # Toggle sort order if same column, otherwise ascending
        if self.sort_column == logical_index:
            self.sort_order = Qt.DescendingOrder if self.sort_order == Qt.AscendingOrder else Qt.AscendingOrder
        else:
            self.sort_column = logical_index
            self.sort_order = Qt.AscendingOrder
        
        # Get the key for this column
        sort_key = self.all_keys[logical_index]
        
        # Sort the filtered data
        reverse = (self.sort_order == Qt.DescendingOrder)
        
        def get_sort_value(row):
            value = row.get(sort_key, '')
            # Handle None values
            if value is None or value == 'NA':
                return ''
            # Try to convert to number for numeric sorting
            try:
                return float(value)
            except (ValueError, TypeError):
                return str(value)
        
        self.filtered_data.sort(key=get_sort_value, reverse=reverse)
        
        # Reset to first page and display
        self.current_page = 0
        self.display_page()
        self.update_pagination_controls()
        
        # Update status
        order_text = "descending" if reverse else "ascending"
        self.statusBar().showMessage(f'Sorted by {sort_key} ({order_text})')
    
    def on_item_changed(self, item):
        """Handle cell edits"""
        if not item:
            return
        
        # Get the actual data row index
        data_row_idx = item.data(Qt.UserRole)
        if data_row_idx is None:
            return
        
        # Get column key
        col_idx = item.column()
        key = self.all_keys[col_idx]
        
        # Update the data
        new_value = item.text()
        
        # Try to convert back to original type
        old_value = self.filtered_data[data_row_idx].get(key, '')
        if isinstance(old_value, (int, float)):
            try:
                new_value = type(old_value)(new_value)
            except ValueError:
                pass
        elif new_value == 'NA':
            new_value = None
        
        # Update both filtered_data and all_data
        self.filtered_data[data_row_idx][key] = new_value
        
        # Find and update in all_data
        for i, row in enumerate(self.all_data):
            if row is self.filtered_data[data_row_idx]:
                self.all_data[i][key] = new_value
                break
        
        # Mark as modified
        self.data_modified = True
        if not self.save_btn.isEnabled():
            self.save_btn.setEnabled(True)
        self.statusBar().showMessage('Data modified - remember to save changes')
        
        # Update info label
        start_idx = self.current_page * self.rows_per_page
        end_idx = min(start_idx + self.rows_per_page, len(self.filtered_data))
        status_text = f'Showing {start_idx + 1}-{end_idx} of {len(self.filtered_data)} records'
        if len(self.filtered_data) != len(self.all_data):
            status_text += f' (filtered from {len(self.all_data)})'
        if self.data_modified:
            status_text += ' - MODIFIED'
        self.info_label.setText(status_text)
    
    def save_file(self):
        """Save changes to GeoJSON file in background"""
        if not self.current_file_path:
            self.save_file_as()
            return
        
        try:
            # Read original file to preserve structure
            with open(self.current_file_path, 'r', encoding='utf-8') as f:
                original_data = json.load(f)
            
            # Create a deep copy of data to avoid modification during save
            import copy
            data_to_save = copy.deepcopy(self.all_data)
            
            # Disable save button during save
            self.save_btn.setEnabled(False)
            self.save_progress_bar.setVisible(True)
            self.save_progress_bar.setValue(0)
            self.save_status_label.setText('Saving...')
            self.statusBar().showMessage('Saving in background...')
            
            # Save in background thread
            self.saver = GeoJSONSaver(self.current_file_path, original_data, data_to_save)
            self.saver.progress.connect(self.on_save_progress)
            self.saver.finished.connect(self.on_save_finished)
            self.saver.error.connect(self.on_save_error)
            self.saver.start()
            
        except Exception as e:
            self.save_progress_bar.setVisible(False)
            self.save_status_label.setText('')
            self.save_btn.setEnabled(True)
            QMessageBox.critical(self, 'Error', f'Failed to start save:\n{str(e)}')
    
    def on_save_progress(self, value, message):
        """Update save progress"""
        self.save_progress_bar.setValue(value)
        self.save_status_label.setText(message)
        self.statusBar().showMessage(message)
    
    def on_save_finished(self, filename):
        """Handle save completion"""
        self.save_progress_bar.setVisible(False)
        self.save_status_label.setText('✓ Saved successfully')
        self.data_modified = False
        self.statusBar().showMessage(f'Successfully saved to {filename}')
        
        # Update info label
        start_idx = self.current_page * self.rows_per_page
        end_idx = min(start_idx + self.rows_per_page, len(self.filtered_data))
        status_text = f'Showing {start_idx + 1}-{end_idx} of {len(self.filtered_data)} records'
        if len(self.filtered_data) != len(self.all_data):
            status_text += f' (filtered from {len(self.all_data)})'
        self.info_label.setText(status_text)
        
        # Clear save status after 3 seconds
        from PyQt5.QtCore import QTimer
        QTimer.singleShot(3000, lambda: self.save_status_label.setText(''))
    
    def on_save_error(self, error_msg):
        """Handle save error"""
        self.save_progress_bar.setVisible(False)
        self.save_status_label.setText('✗ Save failed')
        self.save_btn.setEnabled(True)
        self.statusBar().showMessage('Save failed')
        QMessageBox.critical(self, 'Error', f'Failed to save file:\n{error_msg}')
    
    def save_file_as(self):
        """Save changes to a new file"""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            'Save GeoJSON File',
            '',
            'GeoJSON Files (*.json *.geojson);;All Files (*)'
        )
        
        if file_path:
            self.current_file_path = file_path
            self.save_file()
    
    def merge_polygons_dialog(self):
        """Show dialog to select attribute for merging polygons"""
        if not SHAPELY_AVAILABLE:
            QMessageBox.warning(
                self,
                'Shapely Not Installed',
                'The shapely library is required for polygon operations.\n\n'
                'Install it with: pip install shapely>=2.0.0'
            )
            return
        
        if not self.original_geojson or 'features' not in self.original_geojson:
            QMessageBox.warning(
                self,
                'No Geometry Data',
                'No GeoJSON geometry data found. Please load a valid GeoJSON file with features.'
            )
            return
        
        # Create dialog
        dialog = QDialog(self)
        dialog.setWindowTitle('Merge Polygons by Parent Attribute')
        dialog.setMinimumWidth(400)
        
        layout = QVBoxLayout()
        
        # Instructions
        instructions = QLabel(
            'Select the attribute field to group child polygons.\n'
            'Polygons with the same value will be merged into one parent polygon.'
        )
        instructions.setWordWrap(True)
        layout.addWidget(instructions)
        
        # Attribute selection
        attr_layout = QHBoxLayout()
        attr_layout.addWidget(QLabel('Parent Attribute:'))
        
        attr_combo = QComboBox()
        attr_combo.addItems(self.all_keys)
        # Try to select ADM2_EN or similar if exists
        for i, key in enumerate(self.all_keys):
            if 'ADM2' in key.upper() or 'PARENT' in key.upper():
                attr_combo.setCurrentIndex(i)
                break
        attr_layout.addWidget(attr_combo)
        layout.addLayout(attr_layout)
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        dialog.setLayout(layout)
        
        if dialog.exec_() == QDialog.Accepted:
            parent_attr = attr_combo.currentText()
            self.merge_polygons_by_attribute(parent_attr)
    
    def merge_polygons_by_attribute(self, parent_attr):
        """Merge child polygons that share the same parent attribute value"""
        try:
            features = self.original_geojson.get('features', [])
            
            if not features:
                QMessageBox.warning(self, 'No Features', 'No features found in GeoJSON.')
                return
            
            # Group features by parent attribute
            groups = {}
            for feature in features:
                props = feature.get('properties', {})
                parent_value = props.get(parent_attr, 'Unknown')
                
                if parent_value not in groups:
                    groups[parent_value] = []
                groups[parent_value].append(feature)
            
            # Merge geometries for each group
            merged_features = []
            
            self.statusBar().showMessage('Merging polygons...')
            QApplication.processEvents()
            
            for parent_value, group_features in groups.items():
                if len(group_features) == 1:
                    # Single feature, keep as is
                    merged_features.append(group_features[0])
                else:
                    # Multiple features, merge them
                    geometries = []
                    for feat in group_features:
                        geom = feat.get('geometry')
                        if geom:
                            try:
                                geometries.append(shape(geom))
                            except Exception as e:
                                print(f"Error parsing geometry: {e}")
                    
                    if geometries:
                        # Union all geometries
                        merged_geom = unary_union(geometries)
                        
                        # Create merged feature with properties from first feature
                        merged_feature = {
                            'type': 'Feature',
                            'properties': group_features[0]['properties'].copy(),
                            'geometry': mapping(merged_geom)
                        }
                        merged_features.append(merged_feature)
            
            # Create new GeoJSON
            merged_geojson = {
                'type': 'FeatureCollection',
                'features': merged_features
            }
            
            # Ask user where to save
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                'Save Merged GeoJSON',
                os.path.splitext(self.current_file_path)[0] + '_merged.json' if self.current_file_path else 'merged.geojson',
                'GeoJSON Files (*.json *.geojson);;All Files (*)'
            )
            
            if file_path:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(merged_geojson, f, ensure_ascii=False, indent=2)
                
                self.statusBar().showMessage(
                    f'Merged {len(features)} features into {len(merged_features)} parent polygons'
                )
                QMessageBox.information(
                    self,
                    'Merge Complete',
                    f'Successfully merged {len(features)} child polygons into {len(merged_features)} parent polygons.\n\n'
                    f'Saved to: {os.path.basename(file_path)}'
                )
        
        except Exception as e:
            QMessageBox.critical(
                self,
                'Merge Error',
                f'Failed to merge polygons:\n{str(e)}'
            )
    
    def show_map(self):
        """Display GeoJSON polygons on an interactive map"""
        if not FOLIUM_AVAILABLE:
            QMessageBox.warning(
                self,
                'Folium Not Installed',
                'The folium library is required for map visualization.\n\n'
                'Install it with: pip install folium>=0.14.0 PyQtWebEngine>=5.15.0'
            )
            return
        
        if not self.original_geojson or 'features' not in self.original_geojson:
            QMessageBox.warning(
                self,
                'No Geometry Data',
                'No GeoJSON geometry data found. Please load a valid GeoJSON file with features.'
            )
            return
        
        try:
            features = self.original_geojson.get('features', [])
            
            if not features:
                QMessageBox.warning(self, 'No Features', 'No features found in GeoJSON.')
                return
            
            # Calculate center of all features
            lats, lons = [], []
            for feature in features:
                geom = feature.get('geometry', {})
                if geom and geom.get('type') in ['Polygon', 'MultiPolygon']:
                    coords = geom.get('coordinates', [])
                    if geom['type'] == 'Polygon':
                        for coord in coords[0]:  # First ring (exterior)
                            lons.append(coord[0])
                            lats.append(coord[1])
                    elif geom['type'] == 'MultiPolygon':
                        for polygon in coords:
                            for coord in polygon[0]:  # First ring of each polygon
                                lons.append(coord[0])
                                lats.append(coord[1])
            
            if not lats or not lons:
                QMessageBox.warning(self, 'No Coordinates', 'No valid polygon coordinates found.')
                return
            
            # Calculate center
            center_lat = sum(lats) / len(lats)
            center_lon = sum(lons) / len(lons)
            
            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=8,
                tiles='OpenStreetMap'
            )
            
            # Add GeoJSON layer with styling
            folium.GeoJson(
                self.original_geojson,
                name='GeoJSON Polygons',
                style_function=lambda x: {
                    'fillColor': '#3388ff',
                    'color': '#000000',
                    'weight': 2,
                    'fillOpacity': 0.4
                },
                highlight_function=lambda x: {
                    'fillColor': '#ffff00',
                    'color': '#ff0000',
                    'weight': 3,
                    'fillOpacity': 0.7
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=list(self.all_keys[:5]),  # Show first 5 properties
                    aliases=[f'{key}:' for key in self.all_keys[:5]],
                    localize=True
                )
            ).add_to(m)
            
            # Add layer control
            folium.LayerControl().add_to(m)
            
            # Save map to temporary HTML file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                m.save(f.name)
                map_file = f.name
            
            # Create map viewer window
            self.map_window = QDialog(self)
            self.map_window.setWindowTitle('GeoJSON Map Viewer')
            self.map_window.setGeometry(100, 100, 1000, 700)
            
            layout = QVBoxLayout()
            
            # Info label
            info_label = QLabel(
                f'Displaying {len(features)} polygon(s) | '
                f'Hover over polygons to see properties | '
                f'Scroll to zoom, drag to pan'
            )
            info_label.setStyleSheet('padding: 5px; background-color: #f0f0f0;')
            layout.addWidget(info_label)
            
            # Web view for map
            web_view = QWebEngineView()
            web_view.setUrl(QUrl.fromLocalFile(map_file))
            layout.addWidget(web_view)
            
            # Close button
            close_btn = QPushButton('Close')
            close_btn.clicked.connect(self.map_window.close)
            layout.addWidget(close_btn)
            
            self.map_window.setLayout(layout)
            self.map_window.show()
            
            self.statusBar().showMessage(f'Map displayed with {len(features)} polygons')
        
        except Exception as e:
            QMessageBox.critical(
                self,
                'Map Error',
                f'Failed to display map:\n{str(e)}'
            )


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    viewer = GeoJSONViewer()
    viewer.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
