package org.apache.spark.ml.fed.encryptor.paillier.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.ml.fed.encryptor.paillier.EncryptedNumber;
import org.apache.spark.ml.fed.encryptor.paillier.PaillierContext;
import org.apache.spark.ml.fed.encryptor.paillier.PaillierPrivateKey;
import org.apache.spark.ml.fed.encryptor.paillier.PaillierPublicKey;
import org.apache.commons.codec.binary.Base64;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for common serialisation utils used in the CLI.
 */
public class SerialisationUtil {

//  final static ObjectMapper mapper = new ObjectMapper();

    public static PaillierPublicKey unserialise_public(Map data) {
        // decode the modulus
        BigInteger n = new BigInteger(1, Base64.decodeBase64((String) data.get("n")));

        return new PaillierPublicKey(n);
    }

    public static PaillierPrivateKey unserialise_private(Map data) {

        // First step is to unserialise the Public key
        PaillierPublicKey pub = unserialise_public((Map) data.get("pub"));

        BigInteger lambda = new BigInteger(1, Base64.decodeBase64((String) data.get("lambda")));
        BigInteger mu = new BigInteger(1, Base64.decodeBase64((String) data.get("mu")));

        return new PaillierPrivateKey(pub, lambda);
    }

    public static ObjectNode serialiseEncryptedToObjectNode(EncryptedNumber enc) {
        ObjectNode data;
        ObjectMapper mapper = new ObjectMapper();
        data = mapper.createObjectNode();

        String ff = enc.calculateCiphertext().toString();
        System.out.println("enc.calculateCiphertext().toString() ="+ff);
        System.out.println("enc.getExponent() ="+enc.getExponent());
        data.put("v", ff);
        data.put("e", enc.getExponent());


        JsonNode v = data.get("v");
        JsonNode e = data.get("e");
        System.out.println(v);
        System.out.println(e);
        return data;
    }


    public static SerialisedEncrypted serialiseEncrypted(EncryptedNumber enc) {
        return new SerialisedEncrypted(enc);
    }

    public static EncryptedNumber unserialiseEncrypted(SerialisedEncrypted serialisedEncrypted, PaillierPublicKey pub) {
        HashMap<String, Object> data = serialisedEncrypted.getData();
        BigInteger ciphertext = new BigInteger(data.get("v").toString());
        int exponent = Integer.parseInt(data.get("e").toString());
        PaillierContext context = pub.createSignedContext();
        return new EncryptedNumber(context, ciphertext, exponent);
    }

    public static EncryptedNumber unserialise_encrypted(ObjectNode data, PaillierPublicKey pub) {

        BigInteger ss =  new BigInteger("11655127349817539040913732646035674734195564283189021997848492905295198899703333587219438197695527548082433806936933743210630895602562152915539263765982791038080166422590747890450809666367492698625916149686370304026723314702300154909319647591225929976778294529419016922751663407548009988264655702134285701844764319815485371736349337409009456161955554649078435057577950012825168360686196542887138647724765345795461333440179858981108734251897656332610446812222953251381571577945180388133626528194160486875316606296385843772325703228277913207520961640010275399622736857684654590944179991436054554057431963828756670863831");
        BigInteger ff =  new BigInteger("1040352426951802276595671495727831406027211179933503526442344999046023407728301154262674476806998811178049664215621722200776841974457782431822404554627987901572050902692916890653126790956299017309653140344800417080169697085584538061807030355345490030563920768860185158315340020964492570174372482038334163562722437512953391883914384136331846069329607257798019362834254080759625319451245902765961207114370359270722674430771107812120410296072116783092669810180760870613189336240859366561442347658845834900572752662042927857220424710117313404147002371152384200493638560800935171742897231776877930778396583096335720577849");

        System.out.println(ss.toString());

        System.out.println(data.get("v").toString());
        BigInteger ciphertext = new BigInteger(String.valueOf(data.get("v")));
        System.out.println("ciphertext " + ciphertext);
        int exponent = Integer.parseInt(data.get("e").toString());
        System.out.println("exponent =" + exponent);
        PaillierContext context = pub.createSignedContext();
        System.out.println("context =" + context);
        return new EncryptedNumber(context, ciphertext, exponent);
    }


    public static class SerialisedEncrypted implements Serializable {
        private HashMap<String, Object> data = new HashMap<>(2);

        SerialisedEncrypted(EncryptedNumber enc) {
            data.put("v", enc.calculateCiphertext().toString());
            data.put("e", enc.getExponent());
        }

        public HashMap<String, Object> getData() {
            return data;
        }
    }
}

