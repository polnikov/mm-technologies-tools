import os
import sys
import csv
import datetime
from multiprocessing import freeze_support
from typing import List
from contextlib import ExitStack

import dask
import dask.bag as db
import pandas as pd

from PySide6.QtCore import QSettings, QSize, Qt, QStandardPaths, QObject, QThread, Signal, Slot, QThreadPool
from PySide6.QtGui import QRegularExpressionValidator, QFont, QIcon, QIntValidator
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QLabel,
    QLineEdit,
    QVBoxLayout,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QWidget,
    QTabWidget,
    QFileDialog,
)

from constants import CONSTANTS


basedir = os.path.dirname(__file__)

try:
    from ctypes import windll  # Only exists on Windows.
    myappid = 'akudjatechnology.mm-technologies-tools'
    windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
except ImportError:
    pass

version = '0.0.1'

combobox_style = '''
    QComboBox {
        background-color: #E5FFCC;
        border: 1px solid #E2E2E2;
        border-radius: 5px;
    }
    QAbstractItemView {
        background-color: #E5FFCC
    }
'''
grey_label_style = 'QLabel { background-color: #E0E0E0; border: 0; border-radius: 5px; }'
grey_edit_style = 'QLineEdit { background-color: #E0E0E0; border: 0; border-radius: 5px; }'
green_edit_style = 'QLineEdit { background-color: #E5FFCC; border: 1px solid #E2E2E2; border-radius: 5px; }'

label_width = 200
label_height = 30
align_center = Qt.AlignmentFlag.AlignCenter
align_left = Qt.AlignmentFlag.AlignLeft

save_dir = QStandardPaths.writableLocation(QStandardPaths.StandardLocation.DocumentsLocation)


class PulsWorker(QObject):
    finished = Signal()
    time = Signal(str)

    def __init__(self, files, height_building, width_building, index, dynamic, coef_corr, area_data):
        super().__init__()
        self.files = files
        self.height_building = height_building
        self.width_building = width_building
        self.index = index
        self.dynamic = dynamic
        self.puls_coef_corr = coef_corr
        self.area_data = area_data

    @Slot()
    def run(self):
        threadCount = QThreadPool.globalInstance().maxThreadCount()
        start = datetime.datetime.now()
        delayed_tasks = [dask.delayed(process_file_puls)(file, self.height_building, self.width_building, self.index, self.dynamic, self.puls_coef_corr, self.area_data) for file in self.files]
        dask.compute(*delayed_tasks, scheduler='threads', num_workers=threadCount)
        time = datetime.datetime.now() - start
        self.finished.emit()
        self.time.emit(str(time))


class SortWorker(QObject):
    finished = Signal()
    progress = Signal(str)

    def __init__(self, files):
        super().__init__()
        self.files = files

    @Slot()
    def run(self):
        self.progress.emit('Идёт сортировка и обработка данных ...')
        threadCount = QThreadPool.globalInstance().maxThreadCount()
        delayed_tasks = [dask.delayed(sort_files)(file) for file in self.files]
        dask.compute(*delayed_tasks, scheduler='threads', num_workers=threadCount)
        self.progress.emit('Обработка выполнена')
        self.finished.emit()


class PikWorker(QObject):
    finished = Signal()
    progress = Signal(str)

    def __init__(self, files, height_building, width_building, index, coef_corr, area_data):
        super().__init__()
        self.files = files
        self.height_building = height_building
        self.width_building = width_building
        self.index = index
        self.pik_coef_corr = coef_corr
        self.area_data = area_data

    @Slot()
    def run(self):
        self.progress.emit('Идут вычисления ...')
        if 'max' in self.files[0]:
            processing_type = 'max'
        elif 'min' in self.files[0]:
            processing_type = 'min'

        new_lines = []
        with ExitStack() as stack:
            files = [stack.enter_context(open(i, 'r')) for i in self.files]
            for n, row in enumerate(zip(*files)):
                if n != 0:
                    # print('---', n, row)
                    row = [list(map(lambda x: float(x), i.split())) for i in row]
                    # print(row)
                    match processing_type:
                        case 'max':
                            new_line = max(row, key=lambda x: x[0])
                        case 'min':
                            new_line = min(row, key=lambda x: x[0])
                    new_lines.append(new_line)

        alfa = self.area_data[0]
        dzeta10 = self.area_data[2]
        dimension = self.height_building - self.width_building
        for line in new_lines:
            pressure, Z = line[0], line[3]
            match self.index:
                case 1:
                    result = pressure * (1 + dzeta10 * pow(self.height_building / 10, -alfa)) * self.pik_coef_corr
                case 2:
                    if Z >= dimension:
                        result = pressure * (1 + dzeta10 * pow(self.height_building / 10, -alfa)) * self.pik_coef_corr
                    else:
                        result = pressure * (1 + dzeta10 * pow(self.width_building / 10, -alfa)) * self.pik_coef_corr
                case 3:
                    if Z >= dimension:
                        result = pressure * (1 + dzeta10 * pow(self.height_building / 10, -alfa)) * self.pik_coef_corr
                    else:
                        if Z <= self.width_building:
                            result = pressure * (1 + dzeta10 * pow(self.width_building / 10, -alfa)) * self.pik_coef_corr
                        else:
                            result = pressure * (1 + dzeta10 * pow(Z / 10, -alfa)) * self.pik_coef_corr
            line[0] = result

        file_name = f'{save_dir}\{processing_type}.csv'
        with open(file_name, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow([f'{processing_type.capitalize()}', 'X(m)', 'Y(m)', 'Z(m)'])
            writer.writerows(new_lines)

        self.progress.emit('Завершено')
        self.finished.emit()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.app_title = f'{CONSTANTS.APP_TITLE}_v{version}'
        self.settings = QSettings('akudja.technology', 'mm-technologies-tools')
        self.setWindowTitle(self.app_title)
        self.box_style = 'QGroupBox::title { color: blue; }'

        main_widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.addWidget(self.create_common_widget())
        main_layout.addWidget(self.create_tab_widget())

        main_widget.setLayout(main_layout)
        self.setCentralWidget(main_widget)
        self.adjustSize()
        self.setFixedSize(self.size())

        self.files = False


    def create_common_widget(self) -> QWidget:
        widget = QWidget()
        widget.setStyleSheet('''
            QWidget {
                background-color: #C0C0C0;
            }
        ''')
        vbox = QVBoxLayout()

        hbox_0 = QHBoxLayout()
        hbox_0.setAlignment(align_left)
        add_files_button = QPushButton('Выбрать файлы', self)
        add_files_button.setStyleSheet('''
            QPushButton {
                background-color: #E0E0E0; border-radius: 5px;
            }
            QPushButton:hover {
                border: 0px;
                background: black;
                color: white;
            }
            QPushButton:pressed {
                border: 0px;
                background: black;
                color: white;
            }
        ''')
        add_files_button.setFixedHeight(label_height)
        add_files_button.setFixedWidth(110)
        hbox_0.addWidget(add_files_button)
        self.count_files = QLabel('-')
        hbox_0.addWidget(self.count_files)
        hbox_0.addWidget(QLabel('шт.'))
        add_files_button.clicked.connect(self.add_files)

        hbox_1 = QHBoxLayout()
        hbox_1.setAlignment(align_left)
        hbox_1.setSpacing(2)
        area_label = QLabel('Тип местности')
        area_label.setFixedWidth(175)
        hbox_1.addWidget(area_label)
        self.area_type = QComboBox()
        hbox_1.addWidget(self.area_type)
        area_type = self.area_type
        area_type.setObjectName('area_type')
        area_type.setStyleSheet(combobox_style)
        area_type.setFixedHeight(label_height)
        area_type.setFixedWidth(40)
        area_type.addItems(CONSTANTS.AREA_TYPES.keys())
        area_type.currentTextChanged.connect(self.calculate_zet)
        area_type.currentTextChanged.connect(self.update_area_type_coefs)

        self.alfa_label = QLabel()
        self.alfa_label.setFixedHeight(label_height)
        self.alfa_label.setFixedWidth(45)
        self.alfa_label.setStyleSheet(grey_label_style)
        self.alfa_label.setAlignment(align_center)
        hbox_1.addWidget(self.alfa_label)
        self.alfa_label.setText(str(CONSTANTS.AREA_TYPES.get(self.area_type.currentText())[0]))
        self.alfa_label.setToolTip('⍺')

        self.k10_label = QLabel()
        self.k10_label.setFixedHeight(label_height)
        self.k10_label.setFixedWidth(45)
        self.k10_label.setStyleSheet(grey_label_style)
        self.k10_label.setAlignment(align_center)
        hbox_1.addWidget(self.k10_label)
        self.k10_label.setText(str(CONSTANTS.AREA_TYPES.get(self.area_type.currentText())[1]))
        self.k10_label.setToolTip('k<sub>10</sub>')

        self.dzeta10_label = QLabel()
        self.dzeta10_label.setFixedHeight(label_height)
        self.dzeta10_label.setFixedWidth(45)
        self.dzeta10_label.setStyleSheet(grey_label_style)
        self.dzeta10_label.setAlignment(align_center)
        hbox_1.addWidget(self.dzeta10_label)
        self.dzeta10_label.setText(str(CONSTANTS.AREA_TYPES.get(self.area_type.currentText())[2]))
        self.dzeta10_label.setToolTip('ζ<sub>10</sub>')

        group_box = QGroupBox('Параметры здания')
        group_box.setAlignment(align_center)
        group_box.setStyleSheet(self.box_style)
        hbox_2 = QHBoxLayout()
        hbox_2.setSpacing(5)
        height_building_label = QLabel('Высота, м')
        height_building_label.setFixedWidth(65)
        hbox_2.addWidget(height_building_label)
        self.height_building = QLineEdit()
        hbox_2.addWidget(self.height_building)
        height_building = self.height_building
        height_building.setStyleSheet(green_edit_style)
        height_building.setFixedWidth(50)
        height_building.setFixedHeight(label_height)
        height_building.setAlignment(align_center)
        height_building_regex = r'(?:[0-9]{1,4}(?:\.[0-9]*)?|10000)'
        height_building_validator = QRegularExpressionValidator(height_building_regex)
        height_building.setValidator(height_building_validator)
        height_building.setToolTip('0...10 000')
        height_building.textChanged.connect(self.calculate_index)
        height_building.textChanged.connect(self.calculate_zet)
        height_building.textChanged.connect(self.calculate_dynamic)

        width_building_label = QLabel('Ширина, м')
        width_building_label.setFixedWidth(65)
        hbox_2.addWidget(width_building_label)
        self.width_building = QLineEdit()
        hbox_2.addWidget(self.width_building)
        width_building = self.width_building
        width_building.setStyleSheet(green_edit_style)
        width_building.setFixedWidth(50)
        width_building.setFixedHeight(label_height)
        width_building.setAlignment(align_center)
        width_building_regex = r'(?:[0-9]{1,4}(?:\.[0-9]*)?|10000)'
        width_building_validator = QRegularExpressionValidator(width_building_regex)
        width_building.setValidator(width_building_validator)
        width_building.setToolTip('0...10 000')
        width_building.textChanged.connect(self.calculate_index)

        index_label = QLabel('Индекс')
        index_label.setFixedWidth(45)
        hbox_2.addWidget(index_label)
        self.index = QLineEdit()
        hbox_2.addWidget(self.index)
        index = self.index
        index.setStyleSheet(grey_edit_style)
        index.setDisabled(True)
        index.setFixedWidth(30)
        index.setFixedHeight(30)
        index.setAlignment(align_center)

        group_box.setLayout(hbox_2)

        vbox.addLayout(hbox_0)
        vbox.addLayout(hbox_1)
        vbox.addWidget(group_box)
        widget.setLayout(vbox)
        return widget


    def create_tab_widget(self) -> QWidget:
        tab_widget = QTabWidget(self)
        tab_widget.addTab(self.create_tab1_content(), CONSTANTS.TAB1_TITLE)
        tab_widget.addTab(self.create_tab2_content(), CONSTANTS.TAB2_TITLE)
        return tab_widget


    def create_tab1_content(self) -> object:
        widget = QWidget()
        vbox = QVBoxLayout()
        vbox.setSpacing(10)
        vbox.setAlignment(Qt.AlignmentFlag.AlignTop)

        hbox_1 = QHBoxLayout()
        hbox_1.setAlignment(align_left)
        hbox_1.setSpacing(2)
        wind_area_label = QLabel('Ветровой район')
        wind_area_label.setFixedWidth(label_width)
        hbox_1.addWidget(wind_area_label)
        self.wind_area = QComboBox()
        hbox_1.addWidget(self.wind_area)
        wind_area = self.wind_area
        wind_area.setObjectName('wind_area')
        wind_area.setStyleSheet(combobox_style)
        wind_area.setFixedHeight(label_height)
        wind_area.setFixedWidth(80)
        wind_area.addItems(CONSTANTS.WIND_AREA.keys())
        wind_area.setCurrentText(list(CONSTANTS.WIND_AREA.keys())[1])
        self.wind_area_input = QLineEdit()
        hbox_1.addWidget(self.wind_area_input)
        hbox_1.addWidget(QLabel('Па'))
        wind_area_input = self.wind_area_input
        wind_area_input.setStyleSheet(grey_edit_style)
        wind_area_input.setDisabled(True)
        wind_area_input.setFixedWidth(50)
        wind_area_input.setFixedHeight(label_height)
        wind_area_input.setAlignment(align_center)
        wind_area_input.setText(CONSTANTS.WIND_AREA.get(wind_area.currentText()))
        wind_area_input_validator = QIntValidator()
        wind_area_input_validator.setRange(0, 999999)
        wind_area_input.setValidator(wind_area_input_validator)
        wind_area_input.setToolTip('0...999999')
        wind_area.currentTextChanged.connect(self.activate_wind_area_input)
        wind_area.currentTextChanged.connect(self.set_pressure_by_wind_area)
        wind_area_input.textChanged.connect(self.calculate_e1)

        hbox_2 = QHBoxLayout()
        hbox_2.setAlignment(align_left)
        hbox_2.setSpacing(2)
        coef_corr_label = QLabel('Коэфф. пространственной\nкорреляции')
        coef_corr_label.setFixedWidth(label_width)
        hbox_2.addWidget(coef_corr_label)
        self.puls_coef_corr = QLineEdit()
        hbox_2.addWidget(self.puls_coef_corr)
        coef_corr = self.puls_coef_corr
        coef_corr.setStyleSheet(grey_edit_style)
        coef_corr.setDisabled(True)
        coef_corr.setFixedWidth(65)
        coef_corr.setFixedHeight(30)
        coef_corr.setAlignment(align_center)
        coef_corr.setText(CONSTANTS.COEF_SPATIAL_CORR_PULS)
        self.puls_coef_corr_input = QLineEdit()
        hbox_2.addWidget(self.puls_coef_corr_input)
        coef_corr_input = self.puls_coef_corr_input
        coef_corr_input.setStyleSheet(green_edit_style)
        coef_corr_input.setFixedWidth(65)
        coef_corr_input.setFixedHeight(label_height)
        coef_corr_input.setAlignment(align_center)
        coef_corr_input_regex = r'^(?:[0-9]|[1-9]\d|100)(?:\.\d{1,3})?$'
        coef_corr_input_validator = QRegularExpressionValidator(coef_corr_input_regex)
        coef_corr_input.setValidator(coef_corr_input_validator)
        coef_corr_input.setToolTip('0...100')

        hbox_3 = QHBoxLayout()
        hbox_3.setAlignment(align_left)
        wind_area_label = QLabel('Есть 1-я собственная частота здания?')
        wind_area_label.setFixedWidth(265)
        hbox_3.addWidget(wind_area_label)
        self.frequency = QComboBox()
        hbox_3.addWidget(self.frequency)
        frequency = self.frequency
        frequency.setObjectName('frequency')
        frequency.setStyleSheet(combobox_style)
        frequency.setFixedHeight(label_height)
        frequency.setFixedWidth(60)
        frequency.addItems(CONSTANTS.BUILDING_FREQUENCY.keys())
        frequency.currentTextChanged.connect(self.activate_frequency_calculation)

        hbox_4 = QHBoxLayout()
        hbox_4.setAlignment(align_left)
        self.calculate_puls_button = QPushButton('Рассчитать', self)
        calculate_puls_button = self.calculate_puls_button
        calculate_puls_button.setStyleSheet('''
            QPushButton {
                background-color: #24e034; border-radius: 5px;
            }
            QPushButton:hover {
                border: 0px;
                background: black;
                color: white;
            }
            QPushButton:pressed {
                border: 0px;
                background: black;
                color: white;
            }
        ''')
        calculate_puls_button.setFixedHeight(label_height)
        calculate_puls_button.setFixedWidth(110)
        hbox_4.addWidget(calculate_puls_button)
        calculate_puls_button.clicked.connect(self.process_files_puls_parallel)

        self.status_label_puls = QLabel('', self)
        self.status_label_puls.setVisible(False)
        hbox_4.addWidget(self.status_label_puls)

        vbox.addLayout(hbox_1)
        vbox.addLayout(hbox_2)
        vbox.addLayout(hbox_3)
        vbox.addWidget(self.create_frequency_calculation())
        vbox.addLayout(hbox_4)
        widget.setLayout(vbox)
        return widget


    def create_tab2_content(self) -> object:
        widget = QWidget()
        vbox = QVBoxLayout()
        vbox.setSpacing(10)
        vbox.setAlignment(Qt.AlignmentFlag.AlignTop)

        hbox_1 = QHBoxLayout()
        hbox_1.setAlignment(align_left)
        hbox_1.setSpacing(2)
        coef_corr_label = QLabel('Коэфф. пространственной корреляции')
        coef_corr_label.setFixedWidth(250)
        hbox_1.addWidget(coef_corr_label)
        self.pik_coef_corr = QLineEdit()
        hbox_1.addWidget(self.pik_coef_corr)
        coef_corr = self.pik_coef_corr
        coef_corr.setStyleSheet(grey_edit_style)
        coef_corr.setDisabled(True)
        coef_corr.setFixedWidth(50)
        coef_corr.setFixedHeight(30)
        coef_corr.setAlignment(align_center)
        coef_corr.setText(CONSTANTS.COEF_SPATIAL_CORR_PIK)
        self.pik_coef_corr_input = QLineEdit()
        hbox_1.addWidget(self.pik_coef_corr_input)
        coef_corr_input = self.pik_coef_corr_input
        coef_corr_input.setStyleSheet(green_edit_style)
        coef_corr_input.setFixedWidth(50)
        coef_corr_input.setFixedHeight(label_height)
        coef_corr_input.setAlignment(align_center)
        coef_corr_input_regex = r'^(?:[0-9]|[1-9]\d|100)(?:\.\d{1,3})?$'
        coef_corr_input_validator = QRegularExpressionValidator(coef_corr_input_regex)
        coef_corr_input.setValidator(coef_corr_input_validator)
        coef_corr_input.setToolTip('0...100')

        hbox_2 = QHBoxLayout()
        hbox_2.setAlignment(align_left)
        self.calculate_pik_button = QPushButton('Рассчитать', self)
        calculate_pik_button = self.calculate_pik_button
        calculate_pik_button.setStyleSheet('''
            QPushButton {
                background-color: #24e034; border-radius: 5px;
            }
            QPushButton:hover {
                border: 0px;
                background: black;
                color: white;
            }
            QPushButton:pressed {
                border: 0px;
                background: black;
                color: white;
            }
        ''')
        calculate_pik_button.setFixedHeight(label_height)
        calculate_pik_button.setFixedWidth(110)
        hbox_2.addWidget(calculate_pik_button)
        calculate_pik_button.clicked.connect(self.sort_files_parallel)

        self.status_label_pik = QLabel('', self)
        self.status_label_pik.setVisible(False)
        hbox_2.addWidget(self.status_label_pik)

        vbox.addLayout(hbox_1)
        vbox.addLayout(hbox_2)
        widget.setLayout(vbox)
        return widget


    def add_files(self):
        options = QFileDialog.Options()

        file_dialog = QFileDialog()
        self.files, _ = file_dialog.getOpenFileNames(self, 'Выбрать файлы', '', 'CSV Files (*.csv);;All Files (*)', options=options)
        if self.files:
            self.count_files.setText(str(len(self.files)))


    def activate_wind_area_input(self, value) -> None:
        if value == 'Другой':
            self.wind_area_input.setDisabled(False)
            self.wind_area_input.setStyleSheet(
                green_edit_style
            )
        else:
            self.wind_area_input.setText('')
            self.wind_area_input.setDisabled(True)
            self.wind_area_input.setStyleSheet(
                grey_edit_style
            )


    def set_pressure_by_wind_area(self, value) -> None:
        pressure = CONSTANTS.WIND_AREA.get(self.wind_area.currentText())
        self.wind_area_input.setText(pressure)


    def calculate_index(self, value) -> None:
        height = self.height_building.text()
        width = self.width_building.text()
        if height and width:
            height, width = float(height), float(width)
            if height <= width:
                self.index.setText('1')
            elif height > (2 * width):
                self.index.setText('3')
            else:
                self.index.setText('2')


    def activate_frequency_calculation(self, value) -> None:
        if value == 'Да':
            self.frequency_calculation.setVisible(True)
        else:
            self.frequency_calculation.setVisible(False)
            self.adjustSize()


    def create_frequency_calculation(self) -> object:
        _box = QGroupBox()
        _vbox = QVBoxLayout()
        self.frequency_calculation = _box
        _vbox.setAlignment(align_left)
        _label_width = 260
        _input_width = 60

        _hbox1 = QHBoxLayout()
        _hbox1.setAlignment(align_left)
        label_1 = QLabel('1-я собственная частота здания')
        label_1.setFixedWidth(_label_width)
        _hbox1.addWidget(label_1)
        self.frequency_input = QLineEdit()
        _hbox1.addWidget(self.frequency_input)
        frequency_input = self.frequency_input
        frequency_input.setStyleSheet(green_edit_style)
        frequency_input.setFixedHeight(label_height)
        frequency_input.setFixedWidth(_input_width)
        frequency_input.setAlignment(align_center)
        frequency_input_regex = r'^(?:[0-9]|[1-9]\d|100)(?:\.\d{1,3})?$'
        frequency_input_validator = QRegularExpressionValidator(frequency_input_regex)
        frequency_input.setValidator(frequency_input_validator)
        frequency_input.setToolTip('0...100')
        frequency_input.textChanged.connect(self.calculate_e1)

        _hbox2 = QHBoxLayout()
        _hbox2.setAlignment(align_left)
        label_2 = QLabel('Лог. декремент колебаний, ẟ')
        label_2.setFixedWidth(_label_width)
        _hbox2.addWidget(label_2)
        self.decrement_input = QComboBox()
        _hbox2.addWidget(self.decrement_input)
        decrement_input = self.decrement_input
        decrement_input.addItems(CONSTANTS.DECREMENT)
        decrement_input.setStyleSheet(combobox_style)
        decrement_input.setFixedHeight(label_height)
        decrement_input.setFixedWidth(_input_width)
        decrement_input.currentTextChanged.connect(self.calculate_dynamic)

        _hbox3 = QHBoxLayout()
        _hbox3.setAlignment(align_left)
        label_3 = QLabel('K<sub>(Zэк)</sub>')
        label_3.setFixedWidth(_label_width)
        _hbox3.addWidget(label_3)
        self.zet = QLineEdit()
        _hbox3.addWidget(self.zet)
        zet = self.zet
        zet.setDisabled(True)
        zet.setStyleSheet(grey_edit_style)
        zet.setFixedHeight(label_height)
        zet.setFixedWidth(_input_width)
        zet.setAlignment(align_center)
        zet.textChanged.connect(self.calculate_e1)

        _hbox4 = QHBoxLayout()
        _hbox4.setAlignment(align_left)
        label_4 = QLabel('ε<sub>1</sub>')
        label_4.setFixedWidth(_label_width)
        _hbox4.addWidget(label_4)
        self.e1 = QLineEdit()
        _hbox4.addWidget(self.e1)
        e1 = self.e1
        e1.setDisabled(True)
        e1.setStyleSheet(grey_edit_style)
        e1.setFixedHeight(label_height)
        e1.setFixedWidth(_input_width)
        e1.setAlignment(align_center)
        e1.textChanged.connect(self.calculate_dynamic)
        e1.textEdited.connect(self.calculate_dynamic)

        _hbox5 = QHBoxLayout()
        _hbox5.setAlignment(align_left)
        label_5 = QLabel('Коэфф. динамичности, 𝛏')
        label_5.setFixedWidth(_label_width)
        _hbox5.addWidget(label_5)
        self.dynamic = QLineEdit()
        _hbox5.addWidget(self.dynamic)
        dynamic = self.dynamic
        dynamic.setDisabled(True)
        dynamic.setStyleSheet(grey_edit_style)
        dynamic.setFixedHeight(label_height)
        dynamic.setFixedWidth(_input_width)
        dynamic.setAlignment(align_center)

        _vbox.addLayout(_hbox1)
        _vbox.addLayout(_hbox2)
        _vbox.addLayout(_hbox3)
        _vbox.addLayout(_hbox4)
        _vbox.addLayout(_hbox5)

        _box.setLayout(_vbox)
        _box.setVisible(False)
        return _box


    def calculate_zet(self, value) -> None:
        height = self.height_building.text()
        data = CONSTANTS.AREA_TYPES.get(self.area_type.currentText())
        alfa = data[0]
        k_10 = data[1]
        if all([height, alfa, k_10]):
            height = float(height)
            alfa = float(alfa)
            k_10 = float(k_10)
            result = k_10 * pow((height * 0.8 / 10), (2 * alfa))
            result = '{:.3f}'.format(round(result, 3))
            self.zet.setText(result)
        else:
            self.zet.setText('')


    def update_area_type_coefs(self, value) -> None:
        data = CONSTANTS.AREA_TYPES.get(value)
        self.alfa_label.setText(str(data[0]))
        self.k10_label.setText(str(data[1]))
        self.dzeta10_label.setText(str(data[2]))


    def calculate_e1(self, value) -> None:
        pressure = self.wind_area_input.text()
        zet = self.zet.text()
        frequency = self.frequency_input.text()
        if all([pressure, zet, frequency]):
            pressure, zet, frequency = float(pressure), float(zet), float(frequency)
            result = pow((pressure * zet * 1.4), 0.5) / 940 / frequency
            result = '{:.3f}'.format(round(result, 3))
            self.e1.setText(result)
        else:
            self.e1.setText('')


    def calculate_dynamic(self, value) -> None:
        e1 = self.e1.text()
        if e1:
            e1 = float(e1)
            decrement = self.decrement_input.currentText()
            match decrement:
                case '0.3':
                    result = -1917.9 * pow(e1, 4) + 971.95 * pow(e1, 3) - 187.65 * pow(e1, 2) + 19.745 * e1 + 1
                    result = '{:.3f}'.format(round(result, 3))
                    self.dynamic.setText(result)
                case '0.15':
                    result = -3333.3 * pow(e1, 4) + 1666.7 * pow(e1, 3) - 311.67 * pow(e1, 2) + 31.833 * e1 + 1
                    result = '{:.3f}'.format(round(result, 3))
                    self.dynamic.setText(result)
        else:
            self.dynamic.setText('')


    def get_dynamic(self) -> float | int:
        frequency = CONSTANTS.BUILDING_FREQUENCY.get(self.frequency.currentText())
        if frequency:
            dynamic = self.dynamic.text()
            if dynamic:
                return float(dynamic)
            else:
                return 0
        else:
            return 1


    def get_coef_corr_puls(self) -> float:
        if self.puls_coef_corr_input.text():
            return float(self.puls_coef_corr_input.text())
        else:
            return float(self.puls_coef_corr.text())


    def get_coef_corr_pik(self) -> float:
        if self.pik_coef_corr_input.text():
            return float(self.pik_coef_corr_input.text())
        else:
            return float(self.pik_coef_corr.text())


    def process_files_puls_parallel(self) -> None:
        if not self.files:
            QMessageBox.critical(self, 'Ошибка', 'Нет файлов для расчёта')
        elif not all([self.height_building.text(), self.width_building.text()]):
            QMessageBox.critical(self, 'Ошибка', 'Отсутствуют размеры здания')
        elif not self.get_dynamic():
            QMessageBox.critical(self, 'Ошибка', 'Не рассчитан коэффициент динамичности')
        elif not self.index:
            QMessageBox.critical(self, 'Ошибка', 'Не рассчитан индекс')
        elif 'mean' not in self.files[0]:
            QMessageBox.critical(self, 'Ошибка', 'Похоже, исходные файлы для другого расчёта')
        else:
            self.status_label_puls.setVisible(True)
            self.status_label_puls.setText('Процесс пошёл ...')

            height_building = float(self.height_building.text())
            width_building = float(self.width_building.text())
            index = int(self.index.text())
            dynamic = self.get_dynamic()
            coef_corr = self.get_coef_corr_puls()
            area_data = CONSTANTS.AREA_TYPES.get(self.area_type.currentText())
            files = self.files

            self.puls_thread = QThread()
            self.puls_worker = PulsWorker(files, height_building, width_building, index, dynamic, coef_corr, area_data)
            self.puls_worker.moveToThread(self.puls_thread)

            self.puls_thread.started.connect(self.puls_worker.run)
            self.puls_thread.start()
            self.calculate_puls_button.setDisabled(True)
            self.puls_thread.quit()

            self.puls_worker.finished.connect(self.puls_thread.quit)
            self.puls_worker.finished.connect(self.puls_worker.deleteLater)
            self.puls_worker.time.connect(self.report_puls_finish)
            self.puls_thread.finished.connect(self.puls_thread.deleteLater)
            self.puls_thread.finished.connect(lambda: self.calculate_puls_button.setDisabled(False))


    def report_puls_finish(self, time) -> None:
        self.status_label_puls.setText('Выполнено')


    def sort_files_parallel(self) -> None:
        if not self.files:
            QMessageBox.critical(self, 'Ошибка', 'Нет файлов для расчёта')
        elif not all([self.height_building.text(), self.width_building.text()]):
            QMessageBox.critical(self, 'Ошибка', 'Отсутствуют размеры здания')
        elif not self.index:
            QMessageBox.critical(self, 'Ошибка', 'Не рассчитан индекс')
        elif 'mean' in self.files[0]:
            QMessageBox.critical(self, 'Ошибка', 'Похоже, исходные файлы для другого расчёта')
        else:
            height_building = float(self.height_building.text())
            width_building = float(self.width_building.text())
            index = int(self.index.text())
            coef_corr = self.get_coef_corr_pik()
            area_data = CONSTANTS.AREA_TYPES.get(self.area_type.currentText())
            files = self.files
            self.status_label_pik.setVisible(True)

            self.sort_thread = QThread()
            self.sort_worker = SortWorker(files)
            self.sort_worker.moveToThread(self.sort_thread)

            self.sort_thread.started.connect(self.sort_worker.run)
            self.sort_thread.start()
            self.calculate_pik_button.setDisabled(True)
            self.sort_thread.quit()

            self.sort_worker.finished.connect(self.sort_thread.quit)
            self.sort_worker.finished.connect(self.sort_worker.deleteLater)
            self.sort_worker.progress.connect(self.report_sort_finish)
            self.sort_thread.finished.connect(self.sort_thread.deleteLater)


            self.base_file_thread = QThread()
            self.base_file_worker = PikWorker(files, height_building, width_building, index, coef_corr, area_data)
            self.base_file_worker.moveToThread(self.base_file_thread)

            self.base_file_thread.started.connect(self.base_file_worker.run)
            self.sort_thread.finished.connect(self.base_file_thread.start)
            self.calculate_pik_button.setDisabled(True)
            self.base_file_thread.quit()

            self.base_file_worker.finished.connect(self.base_file_thread.quit)
            self.base_file_worker.finished.connect(self.base_file_worker.deleteLater)
            self.base_file_worker.progress.connect(self.report_sort_finish)
            self.base_file_thread.finished.connect(self.base_file_thread.deleteLater)
            self.base_file_thread.finished.connect(lambda: self.calculate_pik_button.setDisabled(False))


    def report_sort_finish(self, msg) -> None:
        self.status_label_pik.setText(msg)


def process_file_puls(file, height_building, width_building, index, dynamic, coef_corr, area_data) -> None:
    alfa = area_data[0]
    dzeta10 = area_data[2]

    b = db.read_text(file, blocksize=10000000)
    records = b.str.strip().str.split('\t')
    header = records.compute()[0]

    combined_bag = dask.bag.from_sequence(records.compute(), npartitions=1)
    filtered_bag = combined_bag.filter(lambda r: not r[0].startswith(header[0]))

    file_name = file.split('/')[-1].split('_')[0]
    new_file_name = f'{save_dir}\{file_name}_puls.csv'

    new_lines = []
    for line in filtered_bag.compute():
        new_line = process_row_puls(line, index, height_building, width_building, dynamic, coef_corr, alfa, dzeta10)
        new_lines.append(new_line)

    with open(new_file_name, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=' ')
        writer.writerow(['Puls', 'X(m)', 'Y(m)', 'Z(m)'])
        writer.writerows(new_lines)


def process_row_puls(row, index, height_building, width_building, dynamic, coef_corr, alfa, dzeta10) -> List[str]:
    X, Y, Z = row[1], row[2], row[3]
    pressure = float(row[0])
    dimension = height_building - width_building
    match index:
        case 1:
            result = pressure * (dzeta10 * pow(height_building / 10, -alfa)) * dynamic * coef_corr + pressure
        case 2:
            if float(Z) >= dimension:
                result = pressure * (dzeta10 * pow(height_building / 10, -alfa)) * dynamic * coef_corr + pressure
            else:
                result = pressure * (dzeta10 * pow(width_building / 10, -alfa)) * dynamic * coef_corr + pressure
        case 3:
            if float(Z) >= dimension:
                result = pressure * (dzeta10 * pow(height_building / 10, -alfa)) * dynamic * coef_corr + pressure
            else:
                if float(Z) <= width_building:
                    result = pressure * (dzeta10 * pow(width_building / 10, -alfa)) * dynamic * coef_corr + pressure
                else:
                    result = pressure * (dzeta10 * pow(float(Z) / 10, -alfa)) * dynamic * coef_corr + pressure
    new_row = [str(result), X, Y, Z]
    return new_row


def sort_files(file) -> None:
    df = pd.read_csv(file, delimiter=' ')
    headers = list(df.columns)
    # print(headers)
    df_sorted = df.sort_values(by=[headers[1], headers[2], headers[3]])
    if df_sorted['Max of Pressure (Pa)'].str.contains('e').any():
        df_sorted['Max of Pressure (Pa)'] = df_sorted['Max of Pressure (Pa)'].str.split(pat='e', expand=True)[0]

    df_sorted.to_csv(file, index=False, sep='\t')








if __name__ == '__main__':
    import sys
    freeze_support()
    app = QApplication(sys.argv)
    app.setFont(QFont('Consolas', 10))
    app.setStyleSheet('QMessageBox { messagebox-text-interaction-flags: 5; font-size: 13px; }')
    app.setStyle('windowsvista')
    window = MainWindow()
    window.setWindowIcon(QIcon(os.path.join(basedir, 'app.ico')))
    window.setIconSize(QSize(15, 15))
    window.setStyleSheet('QMainWindow::title { background: black; }')
    window.show()
    sys.exit(app.exec())
