package org.talend.components.workday.input;

import lombok.Data;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayout(value = { @GridLayout.Row({ "dataSet" }) })
@Documentation("ADLS input configuration")
public class InputConfiguration {

    @Option
    @Documentation("Dataset")
    private WorkdayDataSet dataSet;
}