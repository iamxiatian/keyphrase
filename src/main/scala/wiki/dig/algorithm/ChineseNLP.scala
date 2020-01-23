package wiki.dig.algorithm

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.Segment

object ChineseNLP {
  val segment: Segment = HanLP.newSegment()
    .enableNameRecognize(true)
    .enableOrganizationRecognize(true)
    .enablePlaceRecognize(true)
    .enableOffset(true)
    .enablePartOfSpeechTagging(true)
    .enableCustomDictionary(true)


}
