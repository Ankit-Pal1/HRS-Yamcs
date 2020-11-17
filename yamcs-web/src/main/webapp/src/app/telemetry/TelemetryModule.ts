import { NgModule } from '@angular/core';
import { SharedModule } from '../shared/SharedModule';
import { CreateDisplayDialog } from './displays/CreateDisplayDialog';
import { DisplayFilePageDirtyDialog } from './displays/DisplayFilePageDirtyDialog';
import { ExportArchiveDataDialog } from './displays/ExportArchiveDataDialog';
import { ImageViewer } from './displays/ImageViewer';
import { MultipleParameterTable } from './displays/MultipleParameterTable';
import { OpiDisplayViewer } from './displays/OpiDisplayViewer';
import { ParameterTableViewer } from './displays/ParameterTableViewer';
import { ParameterTableViewerControls } from './displays/ParameterTableViewerControls';
import { RenameDisplayDialog } from './displays/RenameDisplayDialog';
import { ScriptViewer } from './displays/ScriptViewer';
import { ScrollingParameterTable } from './displays/ScrollingParameterTable';
import { TextViewer } from './displays/TextViewer';
import { UploadFilesDialog } from './displays/UploadFilesDialog';
import { ViewerControlsHost } from './displays/ViewerControlsHost';
import { ViewerHost } from './displays/ViewerHost';
import { ColorPalette } from './parameters/ColorPalette';
import { CompareParameterDialog } from './parameters/CompareParameterDialog';
import { ModifyParameterDialog } from './parameters/ModifyParameterDialog';
import { ParameterAlarmsTable } from './parameters/ParameterAlarmsTable';
import { ParameterDetail } from './parameters/ParameterDetail';
import { ParameterValuesTable } from './parameters/ParameterValuesTable';
import { SelectRangeDialog } from './parameters/SelectRangeDialog';
import { SetParameterDialog } from './parameters/SetParameterDialog';
import { SeverityMeter } from './parameters/SeverityMeter';
import { Thickness } from './parameters/Thickness';
import { DisplayTypePipe } from './pipes/DisplayTypePipe';
import { PacketDownloadLinkPipe } from './pipes/PacketDownloadLinkPipe';
import { routingComponents, TelemetryRoutingModule } from './TelemetryRoutingModule';

const pipes = [
  DisplayTypePipe,
  PacketDownloadLinkPipe,
];

const directives = [
  ViewerControlsHost,
  ViewerHost,
];

const viewers = [
  ImageViewer,
  OpiDisplayViewer,
  ParameterTableViewer,
  ParameterTableViewerControls,
  ScriptViewer,
  TextViewer,
];

@NgModule({
  imports: [
    SharedModule,
    TelemetryRoutingModule,
  ],
  declarations: [
    routingComponents,
    directives,
    pipes,
    viewers,
    ColorPalette,
    CompareParameterDialog,
    CreateDisplayDialog,
    DisplayFilePageDirtyDialog,
    ExportArchiveDataDialog,
    ModifyParameterDialog,
    MultipleParameterTable,
    ParameterDetail,
    ParameterAlarmsTable,
    ParameterValuesTable,
    RenameDisplayDialog,
    ScrollingParameterTable,
    SelectRangeDialog,
    SetParameterDialog,
    SeverityMeter,
    Thickness,
    UploadFilesDialog,
  ],
})
export class TelemetryModule {
}
