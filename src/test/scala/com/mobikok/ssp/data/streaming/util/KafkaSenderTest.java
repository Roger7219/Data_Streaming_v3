package com.mobikok.ssp.data.streaming.util;

/**
 * Created by Administrator on 2017/11/27.
 */
public class KafkaSenderTest {

    public static void main(String[] args) {

        KafkaSender ks = KafkaSender.instance("topic2");

        long i = 0;
        boolean is = true;
        while(is) {
            //System.out.println("01");
            ks.send("| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
//            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");
    //            ks.send( "| 0        | NULL    | 0   | 1048         | 2314   | 11645    | 38          | 99         | 270        | 1           | Mozilla/5.0 (Linux; Android 4.4.2; mbk82_wet_kk Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36  | 95.64.107.215  | 801e8910-4234-4358-82e0-4d23c4545695  | 1.0E-4  |             | 2017-11-26 05:58:07  |            | 2017-11-26 05:58:12  | api          | 3            | 1.0E-4    | 5       | 1       | 0.08         | 0.0        |     |     |       | 1c7cf45b5cea8158  |       |           | 0           |           | v3.0.5  | 358341053700072  | 432113915484988  | 0        | 00000000  |     |     |     | N         | 2017-11-26 06:00:00  | 2017-11-26  |");

            i++;
            try {
//                Thread.sleep(1L);
            } catch (Throwable e) {
                e.printStackTrace();
            }
//            if(i ==1000000)  {
//                is =false;
//            }
        }
        System.out.println("end");
        try {

            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
