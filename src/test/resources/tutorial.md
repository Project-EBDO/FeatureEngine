# Description

This section explains the use of the resources' folder.

## Overview

This folder will contain WAV and text files. WAV files are for audio data and text files will include the expected results of the Scala functions.

## File naming

To add a WAV file, please follow this nomenclature:

1. Specify the type of signal generated in the file (sin, noise, ...)
2. Indicate the sampling rate of the signal
3. Provide the duration



To add a text file, please follow this nomenclature:

1. Specify the type of signal generated in the file (sin, noise, ...)
2. Indicate the sampling rate of the signal
3. Provide the duration
4. Define which function was used
5. List parameters of the function


## Example of file name

WAV file:
`sin_48kHz_10s`  -> type of signal: sinus; sampling rate: 48 kHz; duration: 10 seconds

Text file:
`sin_48kHz_10s_{function}_{parameter1}_{parameter2}`
