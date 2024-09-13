import React, {
    useContext,
    useState,
} from "react";

import Select, { SelectStaticProps } from '@mui/joy/Select';
import Option from '@mui/joy/Option';
import IconButton from '@mui/joy/IconButton';
import CloseRounded from '@mui/icons-material/CloseRounded';

import {StateContext} from "../../contexts/StateContextProvider";
import {
    LOG_LEVEL,
    LogLevelFilter,
    LOG_LEVEL_NAMES_LIST,
} from "../../typings/logs";


/**
 * Renders log level filter
 *
 * @return
 */
const LogLevelSelect = () => {
    const [selectedLogLevels, setSelectedLogLevels] = useState<LogLevelFilter>(null);
    const {changeLogLevelFilter} = useContext(StateContext);
    const action: SelectStaticProps['action'] = React.useRef(null);

    /**
     * Handles changes in the selection of log levels.
     *
     * @param event The synthetic event triggered by the selection change.
     * @param newValue An array of selected values
     */
    const handleChange = (
        event: React.SyntheticEvent | null,
        newValue: Array<string> | null
    ) => {
        // convert strings to numbers.
        const selected: LogLevelFilter = newValue &&
            newValue.map((value) => Number(value));

        console.log(newValue);

        setSelectedLogLevels(selected);
        changeLogLevelFilter(selected);
    };

    return (
        <Select
            action={action}
            multiple={true}
            placeholder="Filter Verbosity"
            size={"sm"}
            sx={{
                borderRadius: 0,
                minWidth: "8rem"
            }}
            // It would be nice for variant=solid; however, JoyUI appears to have
            // where selected values are not highlighted with variant=solid
            // There may be workarounds for this, but left as variant=plain for now.
            variant="plain"
            // Convert selected log levels to strings for value.
            value={selectedLogLevels?.map(String) || []}
            slotProps={{
                listbox: {
                    sx: {
                        width: "100%",
                    },
                },
            }}
            // The following code is responsible for the clear action "x" on
            // select element
            {...(selectedLogLevels && {
                // display the button and remove select indicator
                // when user has selected a value
                endDecorator: (
                <IconButton
                    variant="plain"
                    sx={{ color: 'white' }}
                    onMouseDown={(event) => {
                    // Don't open the popup when clicking on this button
                    event.stopPropagation();
                    }}
                    onClick={() => {
                    //reset log levels to null
                    handleChange(null,null);
                    action.current?.focusVisible();
                    }}
                >
                    <CloseRounded />
                </IconButton>
                ),
                indicator: null,
            })}
            onChange={handleChange}
        >
            {LOG_LEVEL_NAMES_LIST.map((logLevelName, index) => (
                <Option
                    key={logLevelName}

                    // Use index as value.
                    value={index.toString()}
                >
                    {logLevelName}
                </Option>
            ))}
        </Select>
    );
};

export default LogLevelSelect;