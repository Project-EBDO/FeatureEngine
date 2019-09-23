/** Copyright (C) 2017-2018 Project-ODE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.oceandataexplorer.engine.signalprocessing

import org.oceandataexplorer.utils.test.OdeCustomMatchers
import org.scalatest.{FlatSpec, Matchers}

/**
 * Tests for Acoustic Complexity Index class
 *
 * @author Alexandre Degurse
 */
class TestAcousticComplexityIndex extends FlatSpec with Matchers with OdeCustomMatchers {

  /**
   * Maximum error allowed for [[OdeCustomMatchers.RmseMatcher]]
   */
  val maxRMSE = 1E-13

  /*
    np.random.seed(0)
    signal = np.arange(256) + np.random.normal(0.1, 1, size=256)
    fs = 100.0
    windowSize = 16

    spectrum = scipy.signal.stft(
        x=signal, fs=fs, window='boxcar', noverlap=0,
        nperseg=windowSize, nfft=windowSize, detrend=False,
        return_onesided=True, boundary=None,
        padded=False, axis=-1)[-1]
  */

  val spectrumA = Array(
    Array(8.264924942948232, 0.0, -0.2962765239002474, 2.4151368643115614,
    -0.6420892836310677, 0.9951640863954379, -0.4953384824843767,
    0.7793390930627759, -0.38920649975416044, 0.7451456861449368,
    -0.30028547012626666, 0.34533216513647164, -0.478880994009289,
    -0.03873217548011204, -0.4412817240488037, 0.25776000263600096,
    -0.31415464107214497, 0.0), Array(23.798750267251414, 0.0, -0.4276390484326339,
    2.8083910132365433, -0.33823005006493667, 1.4454878405201779,
    -0.4808578127200711, 0.4068700472477823, -0.41466167173994783,
    0.3832040301807558, -0.5770181079146735, 0.5897151710994967,
    -0.06376441260745905, 0.36291816481966865, -0.708403918640122,
    -0.030206120459876917, -0.18352114985411738, 0.0), Array(39.28973739876453, 0.0,
    -0.5203545138264375, 2.354772602264008, -0.9222572077129838, 1.3449374797242197,
    -0.3009827593138277, 0.8498171265381346, -0.3450872310195052,
    0.8261763017388777, -0.7224870268660395, 0.41467106356605254,
    -0.40986494374617605, 0.29317995941255903, -0.41598389563445026,
    0.3546122549117694, -0.8034879901558014, 0.0), Array(55.11087459350978, 0.0,
    -0.7678521342660993, 2.4453306133671764, -0.5091540067562624,
    1.1196730562701593, -0.713347600532919, 0.6388848547452848, -0.5224705653288648,
    0.5110415205130003, -0.4042404795583906, 0.2507639262857373,
    -0.5363603732224819, 0.03361553820871821, -0.534663553127871,
    0.09762351245939938, -0.6485950154819413, 0.0), Array(71.38300137684601, 0.0,
    -0.6396821827221342, 2.4354324933257527, -0.3376149275293118, 1.376981147634145,
    -0.43596487382810273, 0.7800858452309032, -0.47818437568935934,
    0.6329144651939274, -0.4126483692797, 0.28276280727407643, -0.2755197799254146,
    0.17130680565430834, -0.752198209742448, -0.07101584843141273,
    -0.44194979715930316, 0.0), Array(87.98118201959714, 0.0, -0.5494980664777742,
    2.490018761854791, -0.7491710994929954, 1.3768280253117382,
    -0.27804967351584864, 0.769622065966396, -0.6335629219695456,
    0.21156520563032188, -0.7378535151230028, 0.3448983709237371,
    -0.7423826047778006, 0.1927454658646396, -0.4581980497015796,
    0.38528937074181435, -0.7488999982634112, 0.0), Array(104.12162873383203, 0.0,
    -0.181081045844334, 2.7301492135013006, -0.5534388412001322, 1.2015394523741754,
    -0.8235666595253615, 0.8259079010698082, -0.35418110842133643,
    0.35423303178689025, -0.35362358105731007, 0.18707838293467927,
    -1.067650672291117, 0.09106684612455274, -0.34582285711424926,
    0.08137186301014188, -0.6523991822035242, 0.0), Array(119.93646104985301, 0.0,
    -0.43384583292626033, 2.370467527082446, -0.4684021972863454,
    1.1955102371419697, -0.3818634268442659, 0.6966680257643646,
    -0.5275797917334906, 0.7746433227145602, -0.6300538563559454,
    0.11991779468661556, -0.6237833354563371, 0.17692386022074924,
    -0.8636496879261094, 0.20417225288097618, -0.8393304778501971, 0.0),
    Array(135.57926569562977, 0.0, -0.42812484369737125, 2.4631677322851937,
    -0.216184313308819, 1.2956106386478008, -0.5820080910120733, 0.8777014001505972,
    -0.5046014209340939, 0.5054541921373641, -0.3739083068796501,
    0.18738485355424528, -0.3570062083173831, 0.3075185413006377,
    -0.5920327733115307, -0.022917581299098932, -0.6992390236954691, 0.0),
    Array(151.5704824356623, 0.0, -0.3823525736378595, 2.422301310086966,
    -0.2761014730609408, 1.1573137423127595, -0.29702019444407446,
    0.41484294157082763, -0.27024353299901094, 0.6970334902178408,
    -0.1446204682646287, 0.3293575989564345, -0.49544084678648614,
    0.1828681807960284, -0.39403186442534094, -0.27381986568193795,
    -0.5677157535616857, 0.0), Array(167.73375801505878, 0.0, -0.42524517206211243,
    2.582726678670673, -0.3992031893374345, 0.8162214256621174, -0.4136221873344512,
    0.6810731761525056, -0.469651812925008, 0.3189556416868129, -0.7966525055376876,
    0.5326750606209311, -0.46847152958913096, 0.3043136531471935,
    -0.7859956958268994, -0.08623796954411356, -0.6141062805256468, 0.0),
    Array(183.05135134673432, 0.0, -0.3238503072706964, 2.6404379683985235,
    -0.6410291900452726, 1.2027376879406084, -0.6743695750012522,
    0.7355267891434808, -0.36095899783058627, 0.5148022554236817,
    -0.5041762988015719, 0.38785306323828717, -0.2445484731424593,
    0.24980762164192427, -0.8547640977267374, 0.3891565137777879,
    -0.5473671312709882, 0.0), Array(199.8279120860318, 0.0, -0.8530936285678592,
    2.3136541976840697, -0.6358377207512802, 1.271563861912004,
    -0.24040221808538614, 0.6577577680183166, -0.6723992912599499,
    0.7051961878372026, -0.4740598721108253, 0.3694962893076359,
    -0.5917106209338769, 0.20706716585414542, -0.3499695263071898,
    0.058307475252408425, -0.1322491482265491, 0.0), Array(215.7043578807872, 0.0,
    -0.48441682038680384, 2.504588870963514, -0.43364022433920935,
    0.8636943343955048, -0.460871962383395, 1.0228264701419065, -0.6042599050888242,
    0.5548620231640484, -0.42526409760342654, 0.22907985038775414,
    -0.512294622715087, 0.4358855955639891, -0.5552108664001996,
    0.14641049185979416, -0.3725162839100875, 0.0), Array(231.36152528051838, 0.0,
    -0.4323334215816264, 2.630508229446707, -0.5997947805363875, 1.0473731294095645,
    -0.5451167669246888, 0.7599557248912269, -0.44931324642964654,
    0.6890976252416294, -0.8591982799741462, 0.5596969526362224,
    -0.7728085902120345, 0.04713891495305256, -0.4128354161265126,
    -0.09270069617248322, -0.6595212913929345, 0.0), Array(247.45349712781928, 0.0,
    -0.45965966007225145, 2.5779000186536276, -0.45739248292845325,
    1.4092934210383288, -0.5177631520107079, 0.6188953791602174,
    -0.7406197315751228, 0.7974885782552832, -0.36171607023579033,
    0.24398014061071538, -0.7030752019503144, 0.22733266891489134,
    -0.6168793092682564, 0.09024324701215547, -0.2767229372897049, 0.0)
  )

  /*
    np.random.seed(0)
    signal = np.arange(64) + np.random.normal(0.1, 1, size=64)
    fs = 100.0
    windowSize = 15

    spectrum = scipy.signal.stft(
        x=signal, fs=fs, window='boxcar', noverlap=0,
        nperseg=windowSize, nfft=windowSize, detrend=False,
        return_onesided=True, boundary=None,
        padded=False, axis=-1)[-1]
  */
  val spectrumB = Array(
    Array(7.78700831731983, 0.0, -0.27222290605029115, 2.2340754178182918,
      -0.6906360755939576, 0.9842593452943409, -0.5316198085817605,
      0.7092510713660868, -0.2528528482813781, 0.6310961265083486,
      -0.23083713968190742, 0.2548598109813492, -0.6567480105361326,
      0.11668750781337078, -0.3265611969506556, 0.14102090572535447),
    Array(22.29870457720652, 0.0, -0.2618882875577426, 2.570180922496803,
      -0.10763946133086565, 1.0826121119407541, -0.7252783992366959,
      0.7567877630204783, -0.6502544369474063, 0.5602874668209504,
      -0.26105914876052905, 0.08267489085555571, -0.9863540832096592,
      0.14951411008484333, -0.440041307873226, -0.20466024170716418),
    Array(36.86548584209072, 0.0, -0.5873573911668406, 2.2635405532217145,
      -0.0902999209128596, 1.5026952361298849, -0.47159429120751084,
      0.6408832279252171, -0.7256532514206997, 0.37476645515028983,
      -0.6431560513453216, -0.010582043164603937, -0.5340247248822145,
      0.04784962034022492, -0.25318357726145396, -0.050643370453045775),
    Array(51.75547044560132, 0.0, -0.5406852410217178, 2.49740423409731,
      -0.6020558439417473, 1.112783914743024, -0.43501534152557797,
      0.5439351115210499, -0.6424343125717731, 0.5407301073692216,
      -0.4668132209915332, 0.3225943860005938, -0.5435880466541562,
      0.4236178046557305, -0.3161803668997443, 0.22276732270550692)
  )

  it should "compute ACI with 5 windows on spectrumA" in {
    val aciClass = AcousticComplexityIndex(5)
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      1.8478795964334873, 1.2932370887070952, 1.779623265592111,
      1.4861965763159088, 2.0118908926380348
    )
    val expectedAciMainValue = 8.418827419686636

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 8 windows on spectrumA" in {
    val aciClass = AcousticComplexityIndex(8)
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      1.4182803907725403, 0.7165773426920931, 1.2043076477834411,
      1.6026536039217776, 0.7702401168270949, 0.9466739888553486,
      1.3809648253808735, 1.3504728664821704
    )
    val expectedAciMainValue = 9.39017078271534

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 4 windows on spectrumA with frequency bounds -40 Hz" in {
    val nbWindows = 4
    val sampleRate = Some(100.0f)
    val nfft = Some(16)
    val lowFreqBound = None
    val highFreqBound = Some(40.0)

    val aciClass = AcousticComplexityIndex(
      nbWindows, sampleRate, nfft, lowFreqBound, highFreqBound
    )
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      1.117474118396429, 1.4265344192744283, 1.1832740593945221, 1.0914058566759262
    )
    val expectedAciMainValue = 4.818688453741306

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 4 windows on spectrumA with frequency bounds 10-40 Hz" in {
    val nbWindows = 4
    val sampleRate = Some(100.0f)
    val nfft = Some(16)
    val lowFreqBound = Some(10.0)
    val highFreqBound = Some(40.0)

    val aciClass = AcousticComplexityIndex(
      nbWindows, sampleRate, nfft, lowFreqBound, highFreqBound
    )
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      1.117474118396429, 1.4265344192744283, 1.1832740593945221, 1.0914058566759262
    )
    val expectedAciMainValue = 4.818688453741306

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 4 windows on spectrumA with frequency bounds 10- Hz" in {
    val nbWindows = 4
    val sampleRate = Some(100.0f)
    val nfft = Some(16)
    val lowFreqBound = Some(10.0)
    val highFreqBound = None

    val aciClass = AcousticComplexityIndex(
      nbWindows, sampleRate, nfft, lowFreqBound, highFreqBound
    )
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      1.7392508863679852, 2.005679106051335, 1.4881430161530755, 2.0118908926380348
    )
    val expectedAciMainValue = 7.24496390121043

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 8 windows on spectrumA with frequency bounds 10-40 Hz" in {
    val nbWindows = 8
    val sampleRate = Some(100.0f)
    val nfft = Some(16)
    val lowFreqBound = Some(10.0)
    val highFreqBound = Some(40.0)

    val aciClass = AcousticComplexityIndex(
      nbWindows, sampleRate, nfft, lowFreqBound, highFreqBound
    )
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      0.9935109495019765, 0.6070493020603062, 0.8307010372284218,
      1.0490981369680634, 0.561380901619972, 0.8033869991796203,
      0.6687981760480449, 0.7501894586528493
    )
    val expectedAciMainValue = 6.264114961259255

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 8 windows on spectrumA with frequency bounds 12.24-31.424 Hz" in {
    val nbWindows = 8
    val sampleRate = Some(100.0f)
    val nfft = Some(16)
    val lowFreqBound = Some(12.24)
    val highFreqBound = Some(31.424)

    val aciClass = AcousticComplexityIndex(
      nbWindows, sampleRate, nfft, lowFreqBound, highFreqBound
    )
    val acis = aciClass.computeTemporalValues(spectrumA)
    val aciMainValue = aciClass.compute(spectrumA)

    val expectedAcis = Array(
      0.4978986134311729, 0.27141199386549786, 0.17989528525853876,
      0.5079592925894449, 0.4195375077744704, 0.36301600508501636,
      0.5077834544256935, 0.31487787230293374
    )
    val expectedAciMainValue = 3.0623800247327684

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "compute ACI with 2 windows on spectrumB with frequency bounds 12.24-31.424 Hz" in {
    val nbWindows = 2
    val sampleRate = Some(100.0f)
    val nfft = Some(15)
    val lowFreqBound = Some(12.24)
    val highFreqBound = Some(31.424)

    val aciClass = AcousticComplexityIndex(
      nbWindows, sampleRate, nfft, lowFreqBound, highFreqBound
    )
    val acis = aciClass.computeTemporalValues(spectrumB)
    val aciMainValue = aciClass.compute(spectrumB)

    val expectedAcis = Array(
      0.3185170503117893, 0.21134923760242902
    )
    val expectedAciMainValue = 0.5298662879142183

    math.abs(expectedAciMainValue - aciMainValue) should be < maxRMSE
    acis should rmseMatch(expectedAcis)
  }

  it should "raise IllegalArgumentException when the number of temporal windows exceeds spectrum temporal size" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9)
      val acis = aciClass.computeTemporalValues(spectrumA)
    } should have message "Incorrect number of windows (9) for ACI, must be lower than half the spectrum temporal size (16)"
  }

  it should "raise IllegalArgumentException when some but not all optional parameters are given" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9, Some(1.0f), None, Some(1.0))
    } should have message "Incorrect parameters for analysis band specification nfft and samplingRate must be provided."
  }

  it should "raise IllegalArgumentException lowFreqBound is negative" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9, Some(1.0f), Some(256), Some(-1.0))
    } should have message "Incorrect lowFreqBound (-1.0) for ACI, it must be positive and smaller than (0.5)"
  }

  it should "raise IllegalArgumentException lowFreqBound is higher than sampleRate / 2" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9, Some(1.0f), Some(256), Some(1.0))
    } should have message "Incorrect lowFreqBound (1.0) for ACI, it must be positive and smaller than (0.5)"
  }

  it should "raise IllegalArgumentException highFreqBound is higher than sampleRate / 2" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9, Some(1.0f), Some(256), None, Some(1.0))
    } should have message "Incorrect highFreqBound (1.0) for ACI, it must be smaller than (0.5) and higher than (0.0)"
  }

  it should "raise IllegalArgumentException highFreqBound is negative" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9, Some(1.0f), Some(256), None, Some(-1.0))
    } should have message "Incorrect highFreqBound (-1.0) for ACI, it must be smaller than (0.5) and higher than (0.0)"
  }

  it should "raise IllegalArgumentException lowFreqBound is higher than highFreqBound" in {
    the[IllegalArgumentException] thrownBy {
      val aciClass = AcousticComplexityIndex(9, Some(1.0f), Some(256), Some(0.4), Some(0.1))
    } should have message "Incorrect lowFreqBound (0.4) for ACI, it must be positive and smaller than (0.1)"
  }
}
