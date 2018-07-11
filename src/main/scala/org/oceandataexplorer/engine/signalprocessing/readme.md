# Signal Processing package documentation

List of implemented acoustic features:

- Windowing functions (Hamming)
- Fast Fourier Transform
- Power Spectral Density and Power spectrum (using periodogram and Welch estimations)
- Energy (rms measure and log-scaling rms (Sound Pressure Level))

This document explains how to compute these basic acoustic features.

## Glossary

### Data Representation

We have defined some names to designate certain objects.

When given a input signal, it may be segmented a first time. The input signal
being a `Array[Double]` becomes a `Array[Array[Double]]`, an array of `segments`.
This first segmentation enforces the granularity of the results for `Welch`, `SPL` and `TOL`
(eg if the input signal is segmented into `segments` of 1s, we'll have one `SPL` each second).

A second level of segmentation may be applyied as well, each segment will be segmented into `windows`
of `windowSize`. If the signal is segmented only once, the produced objects are called `windows` nonetheless.

### Signal processing definitions

- `WindowFunction` - windowing function for short-term analysis (eg `HammingFunction`)
- `spectrum` - Fourier spectrum computed with a Short-Term Fast Fourier Transform
- `powerSpectrum`
- `powerSpectralDensity`
- `tols` - the third-octave levels over a segment

### Variables

- `fs` - sampling frequency (in Hz)
- `segmentSize` - the size of a segment for the first level of segmentation (in seconds)
- `windowSize` - size of the segmention window for analysis (second level of segmentation, in units)
- `windowOverlap` - the number of samples two consecutive windows have in common (in units)
- `nfft` - the size of the fft-computation window
- `spectrumSize` - the size of the one-sided spectrum
- `lowFreqTOL` - the lower bound of the study range for TOL
- `highFreqTOL` - the upper bound of the study range for TOL
- `frequencyVector` - the frequency vector associated with either a `spectrum`, a `powerSpectrum` or a `powerSpectralDensity`

## Package classes

### Segmentation

The Segmentation class is used to segment a signal into smaller chunks of signal of
user-defined size with or without using overlap/**windowOffset**. These smaller chunks of signal
are called **windows**.

An `windowOffset: Int` can be given to the class, it specifies the distance in samples between
two consecutive windows where as overlap is the number of samples two
consecutive windows have in common whereas overlap is the number of samples
that two consecutive segments have in common.

If the user intends to specify an overlap instead, `windowSize - overlap` is the equivalent windowOffset.

Here is a simple representation of how a signal is segmented without overlap.
To do so, `windowOffset` must be equal to `windowSize`.

```raw
          segmented signal without overlap
--------------------------------------------------------------
|  window0   |  window1    |   window2   |    ....
--------------------------------------------------------------
```

Here is a representation of how the signal is segmented when `windowOffset != windowSize`.

```raw
          segmented signal with overlap
--------------------------------------------------------------
|          window0          |
        |         window1          |
                |         window2          |
--------------------------------------------------------------
|       |       |                 |
  windowOffset  |<-windowOverlap->|
```

_All variables are defined in units, it can't be specified in seconds for instance_

Segmentation parameters, `windowSize` and `windowOffset`, are passed upon instantiation.

Here is an example of how to use this class:

```scala
val segmentationClass = new Segmentation(windowSize, windowOffset)
val windows: Array[Double] = segmentationClass.compute(signal)
```

### WindowFunction

WindowFunction is an abstraction of window functions. A window function is used to make
sure that the data at the edges are zero, so there is no discontinuity, see
[here](https://dsp.stackexchange.com/questions/11312/why-should-one-use-windowing-functions-for-fft)
for more details.

The windows must implement the computation of `windowCoefficients`.

The WindowFunction class provides methods to compute normalization factors for it:

- `powerSpectumNormFactor` computes the window function normalization factor for power spectrum
  given alpha (`sum((windowCoefficients / alpha) ^ 2)`)
- `powerSpectumNormFactor` computes the window function normalization factor for power spectrum

HammingWindow is the only SpectralWindow implemented, there are two kinds of
hamming windows, symmetric and periodic, both kin are implemented.
HammingWindow takes **signalSize** and **hammingType** as parameters.

Here is an example of how to use it:

```scala
val hammingClass = new HammingWindow(signalSize, "symmetric")
val windowedSignal: Array[Double] = hammingClass.applyToSignal(signal)
val hammingNormalizationFactor: Double = hammingClass.normalizationFactor(alpha)
```

### FFT

The FFT class is used to compute **Fast Fourier Transform** over a signal.
The size of the computed spectrum can be specified by the user as **nfft**.

The FFT class computes only one-sided spectrums, complex values are two consecutive
Double (`fft(2*k)` is the real value and `fft(2*k + 1)` is the imaginary value
of the k'th complex value of the spectrum).
A one-sided spectrum contains `nfft/2 + 1` values if nfft is even and `(nfft + 1) / 2`
values if nfft is odd.
Thus the size of the computed spectrum is either `nfft + 2` if nfft is even or
`nfft + 1` if nfft is odd.

_nfft must be equal or higher than the signal length, when higher the signal is zero-padded_

Here is an example of how to use this class:

```scala
val fftClass = new FFT(nfft)
val fft: Array[Double] = fftClass.compute(signal)
```

### Periodogram

The Periodogram class is used to compute **Power Spectral Density** over a spectrum.
The periodogram estimate of the Power Spectrum is `abs(fft) ^ 2`.

This class takes **nfft** and **normalizationFactor** as parameters, thus
an instance of Periodogram computes power spectrum over one-sided spectrum of size nfft+2/nfft+1.
The computed Power Spectrums are normalized with normalizationFactor which is user defined
but is usually `1.0 / (fs * windowNormalizationFactor)` to get a
density and `1.0 / windowNormalizationFactor` to get a power spectrum.

Here is an example of how to use this class:

```scala
val periodogramClass = new Periodogram(nfft, normalizationFactor)
val periodogram: Array[Double] = periodogramClass.compute(fft)
```

### Welch

The Welch class is used to compute Welch power spectrum estimate. This estimate consists in
averaging Periodograms over time for each frequency bin. The given periodograms should be
normalized before being given to this class (i.e. Periodogram class should have
been given the right `normalizationFactor`).

This class takes **nfft** as parameter and expects periodograms of size nfft+2/nfft+1.

Here is an example of how to use this class:

```scala
val welchClass = new WelchSpectralDensity(nfft)
val welch: Array[Double] = welchClass.compute(fft)
```

### Energy

The Energy class provides functions to compute the energy or Sound Pressure
Level of a signal by using either the raw signal, the one-sided spectrum or the PSD
(welch or periodogram) over it.

This class takes **nfft** as parameter and expects spectrum and PSD of length
nfft+2/nfft+1

The energy can either be returned raw (in linear scale) or as a Sound Pressure
Level (in log scale).

Here is an example of how to use this class:

```scala
val energyClass = new Energy(nfft)
val energyRawFromSignal: Array[Double] =
  energyClass.computeRawFromRawSignal(signal)
val energySPLFromFFT: Array[Double] = energyClass.computeSPLFromFFT(fft)
```

## Full example

```scala
// we start be creating a fake signal of 10 seconds
val fs: Float = 16000.0f
val inputSignal: Array[Double] = (1.0 to 10.0 * fs by 1.0).toArray

// segments will be 1 second long
val windowSize = fs.toInt

val windowOverlap = (windowSize * 0.75).toInt

// we take a nfft larger than the segment size, each segment will be
// zero-padded with windowSize zeros
val nfft = 2 * windowSize

// start instantiate classes
val segmentationClass = new Segmentation(windowSize, Some(windowOverlap))
val hammingClass = new HammingFunction(windowSize, "symmetric")

// we'll normalize the spectrum in density
val normalizationFactor = 1.0 / (hammingClass.densityNormalizationFactor(1.0) * fs)

val lowFreqTOL = 800.0
val highFreqTOL = 2000.0

// finish instanciations
val fftClass = new FFT(nfft, fs)
val periodogramClass = new Periodogram(nfft, normalizationFactor, fs)
val welchClass = new WelchSpectralDensity(nfft, fs)
val tolClass = new TOL(nfft, fs, Some(lowFreqTOL), Some(highFreqTOL))
val energyClass = new Energy(nfft)

// we start by segmenting the signal, the signal is 10s long, semgentSize is 1s long
// and windowOverlap is a 75% of windowSize. Thus the 40th segment would start at
// end of the signal (and would be completely empty). The last complete segment
// is 4 segments behind this one, it is the 36th (starting at 0). So there is 37 segments
val windows: Array[Array[Double]] = segmentationClass.compute(signal)

// the rest of the operations are described above, we'll just be
// applying them to each segment

val analysisWindows: Array[Array[Double]] = segments.map(
  segment => hammingClass.applyToSignal(segment)
)

val spectrums: Array[Array[Double]] = windowedSegments.map(
  windowedSegment => fftClass.compute(windowedSegment)
)

val periodograms: Array[Array[Double]] = spectrums.map(
  spectrum => periodogramClass.compute(spectrum)
)

// we have one welch PSD over signal of length nfft+2 (since nfft is even)
val welchs: Array[Double] = welchClass.compute(periodograms)

val tols: Array[Double] = tolClass.compute(welchs)

// we get one SPL for signal
val spl: Double = energyClass.computeSPLFromPSD(welchs)
```
