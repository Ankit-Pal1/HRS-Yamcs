package org.yamcs.mdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.ErrorInCommand;
import org.yamcs.commanding.ArgumentValue;
import org.yamcs.parameter.AggregateValue;
import org.yamcs.parameter.ArrayValue;
import org.yamcs.parameter.Value;
import org.yamcs.utils.BitBuffer;
import org.yamcs.xtce.AggregateDataType;
import org.yamcs.xtce.Argument;
import org.yamcs.xtce.ArgumentEntry;
import org.yamcs.xtce.ArgumentType;
import org.yamcs.xtce.ArrayDataType;
import org.yamcs.xtce.BaseDataType;
import org.yamcs.xtce.CommandContainer;
import org.yamcs.xtce.Container;
import org.yamcs.xtce.DataEncoding;
import org.yamcs.xtce.DataType;
import org.yamcs.xtce.FixedValueEntry;
import org.yamcs.xtce.Member;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterEntry;
import org.yamcs.xtce.ParameterType;
import org.yamcs.xtce.SequenceEntry;

public class MetaCommandContainerProcessor {
    Logger log = LoggerFactory.getLogger(this.getClass().getName());
    TcProcessingContext pcontext;

    MetaCommandContainerProcessor(TcProcessingContext pcontext) {
        this.pcontext = pcontext;
    }

    public void encode(MetaCommand metaCommand) throws ErrorInCommand {
        log.error("inside encode in MetaCommandContainerProcessor start");
        MetaCommand parent = metaCommand.getBaseMetaCommand();
        if (parent != null) {
            encode(parent); // till it get base meta command basically inheriting baseMetaCommand
        }

        CommandContainer container = metaCommand.getCommandContainer();
        if (container == null) {
            throw new ErrorInCommand("MetaCommand has no container: " + metaCommand.getQualifiedName());
        }

        if (parent == null) { // strange case for inheriting only the container without a command
            Container baseContainer = container.getBaseContainer();
//            log.error(baseContainer +"     " ); // null
            if (baseContainer != null) {
                encode(baseContainer);
            }
        }
//        log.error(container.getEntryList() +"  1   " ); //[ArgumentEntry position:0, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Version argType:IntegerArgumentType name:CCSDS_Version_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 3, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgumentEntry position:1, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Type argType:BooleanArgumentType name:CCSDS_Type_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgumentEntry position:2, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Sec_Hdr_Flag argType:BooleanArgumentType name:CCSDS_Sec_Hdr_Flag_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgumentEntry position:3, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_APID argType:IntegerArgumentType name:CCSDS_APID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 11, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgumentEntry position:4, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Group_Flags argType:EnumeratedArgumentType: {0=(0=Continuation), 1=(1=First), 2=(2=Last), 3=(3=Standalone)} encoding:IntegerDataEncoding[sizeInBits: 2, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], FixedValueEntry position:5, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, sizeInBits: 14, binaryValue: 0000, FixedValueEntry position:6, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, sizeInBits: 16, binaryValue: 0000]    1

        for (SequenceEntry se : container.getEntryList()) { // picking up ArgumentEntry and FixedValueEntry defined in xml file
//            log.error(se + "    " + count++);
            // ArgumentEntry position:0, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Version argType:IntegerArgumentType name:CCSDS_Version_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 3, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]    0
            // ArgumentEntry position:1, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Type argType:BooleanArgumentType name:CCSDS_Type_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]    1
            // ArgumentEntry position:2, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Sec_Hdr_Flag argType:BooleanArgumentType name:CCSDS_Sec_Hdr_Flag_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]    2
            // ArgumentEntry position:3, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_APID argType:IntegerArgumentType name:CCSDS_APID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 11, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]    3
            // ArgumentEntry position:4, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, argument: ArgName: CCSDS_Group_Flags argType:EnumeratedArgumentType: {0=(0=Continuation), 1=(1=First), 2=(2=Last), 3=(3=Standalone)} encoding:IntegerDataEncoding[sizeInBits: 2, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]    4
            //FixedValueEntry position:5, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, sizeInBits: 14, binaryValue: 0000    5
            //FixedValueEntry position:6, container:CCSDSPacket locationInContainer:0 from:PREVIOUS_ENTRY, sizeInBits: 16, binaryValue: 0000    6
            int size = 0;
            BitBuffer bitbuf = pcontext.bitbuf; //  org.yamcs.utils.BitBuffer@34d8f7a4
//            log.error(bitbuf.getPosition() +":"+ se.getLocationInContainerInBits()+":" +":"+ bitbuf.getPosition() + se.getLocationInContainerInBits()); //  8:0::80 11:0::110 12:0::120 13:0::130  24:0::240  26:0::260  40:0::400 56:0::560 72:0::720 104:0::1040 112:0::1120 152:0::1520 160:0::1600
            switch (se.getReferenceLocation()) { // PREVIOUS_ENTRY and bitbuf.getByte(): 0
            case PREVIOUS_ENTRY:
                bitbuf.setPosition(bitbuf.getPosition() + se.getLocationInContainerInBits());
                break;
            case CONTAINER_START:
                bitbuf.setPosition(se.getLocationInContainerInBits());
            }
            if (se instanceof ArgumentEntry) {
                log.error("inside ArgumentEntry" + ":" );
                fillInArgumentEntry((ArgumentEntry) se, pcontext);
                size = (bitbuf.getPosition() + 7) / 8;
                log.error("inside ArgumentEntry" + ":"+ size );
            } else if (se instanceof FixedValueEntry) {
//                log.error("inside FixedValueEntry" + ":" );
                fillInFixedValueEntry((FixedValueEntry) se, pcontext);
                size = (bitbuf.getPosition() + 7) / 8;
//                log.error("inside FixedValueEntry" + ":"+ size );
            } else if (se instanceof ParameterEntry) { // not called
//                log.error("inside ParameterEntry" + ":" );
                fillInParameterEntry((ParameterEntry) se, pcontext);
                size = (bitbuf.getPosition() + 7) / 8;
//                log.error("inside ParameterEntry" + ":" + size );
            }
            if (size > pcontext.getSize()) {
                pcontext.setSize(size);
            }
        }
    }

    private void encode(Container container) {
        Container baseContainer = container.getBaseContainer();
        if (baseContainer != null) {
            encode(baseContainer);
        }
        for (SequenceEntry se : container.getEntryList()) {
            int size = 0;
            BitBuffer bitbuf = pcontext.bitbuf;
            switch (se.getReferenceLocation()) {
            case PREVIOUS_ENTRY:
                bitbuf.setPosition(bitbuf.getPosition() + se.getLocationInContainerInBits());
                break;
            case CONTAINER_START:
                bitbuf.setPosition(se.getLocationInContainerInBits());
            }
            if (se instanceof ArgumentEntry) {
                fillInArgumentEntry((ArgumentEntry) se, pcontext);
                size = (bitbuf.getPosition() + 7) / 8;
            } else if (se instanceof FixedValueEntry) {
                fillInFixedValueEntry((FixedValueEntry) se, pcontext);
                size = (bitbuf.getPosition() + 7) / 8;
            } else if (se instanceof ParameterEntry) {
                fillInParameterEntry((ParameterEntry) se, pcontext);
                size = (bitbuf.getPosition() + 7) / 8;
            }
            if (size > pcontext.getSize()) {
                pcontext.setSize(size);
            }
        }
    }

    private void fillInArgumentEntry(ArgumentEntry argEntry, TcProcessingContext pcontext) {
        log.error("inside fillInArgumentEntry method in MetaCommandContainerProcessor start");
        Argument arg = argEntry.getArgument(); // ArgName: Value argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)
//        log.error("inside fillInArgumentEntry method in MetaCommandContainerProcessor 1::getArgument()::" + arg); // // ArgName: Value argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)
        ArgumentValue argValue = pcontext.getCmdArgument(arg); // name: Value engValue: {3000}
//        log.error("inside fillInArgumentEntry method in MetaCommandContainerProcessor 2::getCmdArgument(arg)::" + argValue); // name: Value engValue: {3000}
        if (argValue == null) {
            throw new IllegalStateException("No value for argument " + arg.getName());
        }
        Value engValue = argValue.getEngValue(); // 3000
//        log.error("inside fillInArgumentEntry method in MetaCommandContainerProcessor 3::getEngValue();::" + engValue); // 3000
        ArgumentType atype = arg.getArgumentType();
//        log.error("inside fillInArgumentEntry method in MetaCommandContainerProcessor 4::getArgumentType();" + "-- :" + atype); //StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0) and for int = IntegerArgumentType name:CCSDS_APID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 11, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]
        Value rawValue = pcontext.argumentTypeProcessor.decalibrate(atype, engValue); // engValue = 0/true/false/101/Standalone/2000/3000
//      rawValue on everyLoop = 0 1 0 101 3 4 2000 3000 picked from xtce.xml file and from frontend
        argValue.setRawValue(rawValue);
        encodeRawValue(arg.getName(), atype, rawValue, pcontext);
    }

    private void encodeRawValue(String argName, DataType type, Value rawValue, TcProcessingContext pcontext) {
        log.error("inside encodeRawValue in MetaCommandContainerProcessor:: start");
        if (type instanceof BaseDataType) { // called
            DataEncoding encoding = ((BaseDataType) type).getEncoding();
            // encoding values
            // IntegerDataEncoding[sizeInBits: (3,1,1,11,2,16), byteOrder: BIG_ENDIAN, encoding:UNSIGNED]
            // StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)
            if (encoding == null) {
                throw new CommandEncodingException("No encoding available for type '" + type.getName()
                        + "' used for argument '" + argName + "'");
            }
            pcontext.deEncoder.encodeRaw(encoding, rawValue); // Go to DataEncodingEncoder
        } else if (type instanceof AggregateDataType) { // Not called
            AggregateDataType aggtype = (AggregateDataType) type;
            AggregateValue aggRawValue = (AggregateValue) rawValue;
            for (Member aggm : aggtype.getMemberList()) {
                Value mvalue = aggRawValue.getMemberValue(aggm.getName());
                encodeRawValue(argName + "." + aggm.getName(), aggm.getType(), mvalue, pcontext);
            }
        } else if (type instanceof ArrayDataType) { // Not called
            ArrayDataType arrtype = (ArrayDataType) type;
            DataType etype = arrtype.getElementType();
            ArrayValue arrayRawValue = (ArrayValue) rawValue;
            for (int i = 0; i < arrayRawValue.flatLength(); i++) {
                Value valuei = arrayRawValue.getElementValue(i);
                encodeRawValue(argName + arrayRawValue.flatIndexToString(i), etype, valuei, pcontext);
            }
        } else {
            throw new CommandEncodingException("Arguments or parameters of type " + type + " not supported");
        }
    }

    private void fillInParameterEntry(ParameterEntry paraEntry, TcProcessingContext pcontext) {
        Parameter para = paraEntry.getParameter();
        Value paraValue = pcontext.getRawParameterValue(para);
        if (paraValue == null) {
            throw new CommandEncodingException("No value found for parameter '" + para.getName() + "'");
        }

        Value rawValue = paraValue; // TBD if this is correct
        ParameterType ptype = para.getParameterType();
        encodeRawValue(para.getQualifiedName(), ptype, rawValue, pcontext);
    }

    private void fillInFixedValueEntry(FixedValueEntry fve, TcProcessingContext pcontext) {
        int sizeInBits = fve.getSizeInBits();
        final byte[] v = fve.getBinaryValue();

        int fb = sizeInBits & 0x07; // number of bits in the leftmost byte in v
        int n = (sizeInBits + 7) >>> 3;
        BitBuffer bitbuf = pcontext.bitbuf;
        int i = v.length - n;
        if (fb > 0) {
            bitbuf.putBits(v[i++], fb);
        }
        while (i < v.length) {
            bitbuf.putBits(v[i++], 8);
        }
    }
}
