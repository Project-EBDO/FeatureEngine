# Signal Processing package documentation

This programmer-oriented documentation describes our scala implementation of the
following signal processing tools and features:

- Windowing functions (e.g., Hamming, Hann, Backman, ...)
- Spectrum (using Fast Fourier Transform)
- Power Spectral Density and Power spectrum (using periodogram and Welch estimations)
- Energy (e.g., root mean square (rms) measure or Sound Pressure Level, i.e., log-scaling rms)

It also provides a step-by-step code tutorial to use them.

## Glossary

### Two-level segmentation

When given an input signal, two successive levels of temporal segmentation are applied, corresponding to two different time scales of analysis.

- `first level` - this first segmentation divides the input signal into shorter `segments` of `segmentSize`, defined in seconds or in number of samples. The input signal being a `Array[Double]` becomes a `Array[Array[Double]]`, i.e., an array of `segments`. It enforces the granularity of "integrating features" like `Welch`, `SPL` and `TOL` (e.g., if the input signal is segmented into `segments` of 1s, we will have one `SPL` each second)

- `second level` - this second level divides each segment into shorter `windows` of `windowSize`, defined in signal-samples. Windows correspond to the standard segmentation for short-term Fourier analysis, enforcing the granularity of the `spectrum` feature

If the signal is segmented only once, the produced objects are called `windows`
nonetheless. Note that only the second level has the specific segmentation
properties of windowing and overlap.

### Signal processing definitions

- `WindowFunction` - windowing function for short-term analysis (e.g., `HammingWindowFunction`, `BlackmanWindowFunction`, `HannWindowFunction`, ...)
- `spectrum` - spectral estimate of an input signal based on the Fast Fourier Transform
- `powerSpectrum` - power spectral estimate based on the squared amplitude of `spectrum`. The power spectrum of a timeserie describes the distribution of power into frequency components composing that signal
- `powerSpectralDensity` - spectral density estimate based on a normalization by the sample frequency of `powerSpectrum`, so it refers to the spectral energy distribution that would be found per unit of time. See [here](https://en.wikipedia.org/wiki/Spectral_density) for more details
- `tols` - power spectral estimate integrated over a third-octave filter, based on `powerSpectrum`
- `energy` - energy measure of the input signal, i.e., root mean square (rms) measure or Sound Pressure Level (log-scaling rms)

### Variables

#### General

- `fs` - raw signal sampling frequency (in Hz)

_Note: in the later, one signal sample accounts for 1/fs second_

#### Segmentation

- `segmentSize` - size of a segment, i.e., time scale of the first segmentation level (in seconds)
- `windowSize` - size of a window, i.e., time scale of the second segmentation level (in samples)
- `windowOverlap` - number of samples that two consecutive windows have in common (in samples)
- `windowOffset` - distance in samples between two consecutive windows (in samples, relates to windowOverlap: windowOffset = windowSize -windowOverlap)

#### Feature

- `nfft` - number of points in the FFT (in samples). `nfft` being a power of 2 speeds up the computation
- `spectrumSize` - size of the one-sided spectrum (in samples)
- `lowFreqTOL` - lower bound of the study range for TOL (in Hz)
- `highFreqTOL` - upper bound of the study range for TOL (in Hz)
- `frequencyVector` - frequency vector associated with either a `spectrum`, a `powerSpectrum` or a `powerSpectralDensity` (in Hz)

## Classes details

### Segmentation

First, note that the first segmentation level described earlier can be generated
by the hadoop WavPCMInputFormat (defined in ODE
[hadoop-io-extensions](https://github.com/Project-ODE/hadoop-io-extensions)).
If you don't use this hadoop input format, you
need to enforce the first-level segmentation as this class corresponds.

This Segmentation class corresponds to our
second level of segmentation, i.e., dividing each Hadoop-based segment into
short-term **windows**.

Segmentation parameters, `windowSize` and `windowOverlap`, are passed upon
instantiation.

Here is a simple representation of how a signal is segmented without overlap.

```raw
          segmented signal without overlap
--------------------------------------------------------------
|  window0   |  window1    |   window2   |    ....
--------------------------------------------------------------
```

Here is a representation of how the signal is segmented when `windowOveralp != 0`.

```raw
          segmented signal with overlap
--------------------------------------------------------------
|            window0           |
        |           window1           |
                |           window2           |
--------------------------------------------------------------
                | <- windowOverlap -> |
```

_Note: windowSize and windowOverlap are defined in samples, not in time (seconds or milli)_

Here is an example of how to use this class:

```scala
// 75% overlap here
val segmentationClass = new Segmentation(windowSize=400, windowOverlap=300)
val windows: Array[Double] = segmentationClass.compute(signal)
```

### WindowFunction

Window functions are used to deal with discontinuity at the edges of a window.
See [here](https://dsp.stackexchange.com/questions/11312/why-should-one-use-windowing-functions-for-fft)
for more details.

To facilitate implementing and using various window types, we have defined:

- WindowFunction trait - Needs a `windowSize` and `windowCoefficients` to be instantiated, and provides a function to apply the window-function to a signal, and two normalization factors the `powerSpectumNormFactor` and `powerSpectralDensityNormFactor`.

- abstract CosineWindowFunction extends the WindowFunction trait, and provides window-coefficients for cosine-coefficients and a window-type, Periodic or Symmetric (see [here](https://en.wikipedia.org/wiki/Window_function#Symmetry) for more details). The three windows we instantiate (Hamming, Hann, Blackman) extend the CosineWindowFunction with predefined parameters (see [here](https://en.wikipedia.org/wiki/Window_function#Cosine-sum_windows) for more details).

To each window function is associated a set of pre-defined `windowCoefficients`,
used to compute them. The WindowFunction class also provides methods to compute
normalization factors:

- for power spectrum, called `powerSpectumNormFactor`, and defined as (`sum(windowCoefficients / alpha) ^ 2`), given alpha
- for power spectral density, called `powerSpectralDensityNormFactor`, and defined as (`sum((windowCoefficients / alpha) ^ 2)`), given alpha

Also, there are two types of hamming windows, symmetric and periodic, and both
are implemented and can be passed upon instantiation. HammingWindow also takes
**signalSize** and **windowType** as parameters.

So far, `HammingWindowFunction`, `BlackmanWindowFunction` and `HannWindowFunction`
are implemented.

Here is an example of how to use it:

```scala
val hammingClass = new HammingWindowFunction(signalSize=400, Periodic)
val windowedSignal: Array[Double] = hammingClass.applyToSignal(signal)
val hammingNormalizationFactor: Double = hammingClass.densityNormalizationFactor(alpha=1.0)
```

### FFT

The FFT class is used to compute **Fast Fourier Transform** over a signal. The
size of the computed spectrum can be specified by the user with **nfft**.

The FFT class computes one-sided spectrum. It means half of the spectrum a
double-sided computation would provide is given as result here. This is valid
since real-only-FFTs (meaning FFTs computed on real-values by opposition to
imaginary ones, which is always the case for real-life signal) have the
property of being mirrored, therefore only half of the full result is needed
(see https://dsp.stackexchange.com/questions/4825/why-is-the-fft-mirrored).
The result provided by the FFT function contains complex numbers represented as two
consecutive Double where `fft(2*k)` is the real value and `fft(2*k + 1)` is the
imaginary value of the k'th complex value of the spectrum.

A one-sided spectrum contains `nfft/2 + 1` complex values if nfft is even
and `(nfft + 1) / 2` complex values if nfft is odd while a two-sided spectrum
would be of size `nfft`.

Due to the way complex values are handled, the size of the computed
(one-sided) spectrum is either `nfft + 2` if nfft is even or `nfft + 1` if nfft
is odd.

_nfft must be equal or higher than the signal length. When higher, the signal is zero-padded_

Here is an example of how to use this class:

```scala
// signal is an Array[Double] containing real values
val fftClass = new FFT(nfft=256, fs=32768.0f)
val fft: Array[Double] = fftClass.compute(signal)
```

### Periodogram

 The periodogram estimates the **Power Spectrum** based on **spectrum**, i.e.,
`abs(spectrum) ^ 2`.

This class takes **nfft** and **normalizationFactor** as parameters.
Periodogram computes power spectrum over one-sided spectrum of size
`nfft+2` or `nfft+1`.

Depending on the `normalizationFactor` value, we can then compute the normalized
**Power Spectrum** (simply called power spectrum), with `normalizationFactor =
1.0 / windowNormalizationFactor`, and/or the **Power Spectral Density**, with
`normalizationFactor = 1.0 / (fs * windowNormalizationFactor)`

Here is an example of how to use this class:

```scala
val periodogramClass = new Periodogram(nfft=256,
  normalizationFactor=1.0/(fs * hammingNormalizationFactor), fs=32768.0f)
val periodogram: Array[Double] = periodogramClass.compute(fft)
```

### Welch

The Welch class is used to compute Welch power spectrum estimate. This estimate
consists in averaging Periodograms over time for each frequency bin. The given
periodograms should be normalized before being given to this class (i.e.,
Periodogram class should have been given the right `normalizationFactor`).

This class takes **nfft** as parameter and expects periodograms of size
`nfft+2` or `nfft+1`.

Here is an example of how to use this class:

```scala
val welchClass = new WelchSpectralDensity(nfft=256, fs=32768.0f)
val welch: Array[Double] = welchClass.compute(fft)
```

### TOL

_Will be documented soon._

### Energy

 The Energy class provides functions to compute the root-mean-square (rms)
energy or Sound Pressure Level of a signal by using either the raw signal, the
one-sided spectrum or the PSD (welch or periodogram) over it.

This class takes **nfft** as parameter and expects spectrum and PSD of length
nfft+2 or nfft+1

The energy can either be returned as a raw measure in linear scale or as a
decibel measure in log scale, called Sound Pressure Level.

Here is an example of how to use this class:

```scala
val energyClass = new Energy(nfft=256)
val energyRawFromSignal: Array[Double] =
  energyClass.computeRawFromRawSignal(signal)
val energySPLFromFFT: Array[Double] = energyClass.computeSPLFromFFT(fft)
```

## Full example

_This example demonstrates a use of the signal processing package using a single
level of segmentation_

```scala
import org.oceandataexplorer.engine.signalprocessing._
import org.oceandataexplorer.engine.signalprocessing.windowfunctions._
import WindowFunctionTypes.{Symmetric, Periodic}

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
val hammingClass = new HammingWindowFunction(windowSize, Periodic)

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
// end of the signal (and would be completely empty). The last complete window
// is 4 segments behind this one, it is the 36th (starting at 0). So there is 37 windows
val windows: Array[Array[Double]] = segmentationClass.compute(inputSignal)

// the rest of the operations are described above, we'll just be
// applying them to each segment

val analysisWindows: Array[Array[Double]] = windows.map(
  segment => hammingClass.applyToSignal(segment)
)

val spectrums: Array[Array[Double]] = analysisWindows.map(
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
