package org.yamcs.mdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.yamcs.ErrorInCommand;
import org.yamcs.ProcessorConfig;
import org.yamcs.commanding.ArgumentValue;
import org.yamcs.parameter.Value;
import org.yamcs.utils.BitBuffer;
import org.yamcs.xtce.Argument;
import org.yamcs.xtce.ArgumentAssignment;
import org.yamcs.xtce.CommandContainer;
import org.yamcs.xtce.Comparison;
import org.yamcs.xtce.ComparisonList;
import org.yamcs.xtce.Container;
import org.yamcs.xtce.MatchCriteria;
import org.yamcs.xtce.MetaCommand;
import org.yamcs.xtce.OperatorType;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterInstanceRef;

public class MetaCommandProcessor {
    final ProcessorData pdata; // Contains a cache of encoders, decoders, calibrators and converts raw values into engineering values

    public MetaCommandProcessor(ProcessorData pdata) {
        this.pdata = pdata;
    }

    public CommandBuildResult buildCommand(MetaCommand mc, Map<String, Object> argAssignmentList) //{Value=3000, Port=2000}
            throws ErrorInCommand {
        return buildCommand(pdata, mc, argAssignmentList);
    }

    public static CommandBuildResult buildCommand(ProcessorData pdata, MetaCommand mc,
            Map<String, Object> argAssignmentList) throws ErrorInCommand {
        System.out.println("inside buildCommand with processorData(pdata) MetaCommandProcessor start");
        if (mc.isAbstract()) {
            throw new ErrorInCommand("Not building command " + mc.getQualifiedName() + " because it is abstract");
        }
        ProcessorConfig procConf = pdata.getProcessorConfig();
        Map<Parameter, Value> params = new HashMap<>();

        CommandContainer cmdContainer = mc.getCommandContainer();
        //System.out.println("inside buildCommand with processorData(pdata) MetaCommandProcessor" +":" + procConf + ":" + procConf.getMaxCommandSize() +":" + params +":" + cmdContainer.getQualifiedName()+":" + cmdContainer.getName() );// in procConf alarm record related info
        // rest info are :4096:{}:/myproject/PingCommand:PingCommand

        if (cmdContainer == null && !procConf.allowContainerlessCommands()) {
            throw new ErrorInCommand("MetaCommand " + mc.getName()
                    + " has no container (and the processor option allowContainerlessCommands is set to false)");
        }
        if (cmdContainer != null) {
            collectParameters(cmdContainer, params); // collecting parameters but params:{} null
        }
        BitBuffer bitbuf = new BitBuffer(new byte[procConf.getMaxCommandSize()]); // maxTcSize = 4096;
        System.out.println("inside buildCommand, bitBuffer info : " +bitbuf +":"+ bitbuf.getByte()+":"+bitbuf.getPosition()+":"+bitbuf.arrayLength()+":" );
        //inside buildCommand, bitBuffer info : org.yamcs.utils.BitBuffer@71531d4e:0:8:4096:
        TcProcessingContext pcontext = new TcProcessingContext(mc, pdata, params, bitbuf, 0); // In this it is loading data from xtce.xml and arguments and parameters also reading 2000 3000 argumentsType and more

        Map<String, Object> argAssignment = new HashMap<>(argAssignmentList); // {Value=3000, Port=2000}

        List<ArgumentAssignment> inheritedAssignment = mc.getEffectiveArgumentAssignmentList();
        // inheritedAssignment => [CCSDS_Version=0, CCSDS_Type=TC, CCSDS_Sec_Hdr_Flag=NotPresent, CCSDS_APID=101, CCSDS_Group_Flags=Standalone, Packet_ID=4]

        for (ArgumentAssignment aa : inheritedAssignment) {
            if (argAssignment.containsKey(aa.getArgumentName())) {
                throw new ErrorInCommand("Cannot overwrite the argument " + aa.getArgumentName()
                        + " which is defined in the inheritance assignment list");
            }
            argAssignment.put(aa.getArgumentName(), aa.getArgumentValue());
            // argAssignment => {CCSDS_APID=101, CCSDS_Version=0, CCSDS_Sec_Hdr_Flag=NotPresent, Port=2000, Value=3000, CCSDS_Type=TC, CCSDS_Group_Flags=Standalone, Packet_ID=4}:
        }


        collectAndCheckArguments(pcontext, argAssignment);  // {CCSDS_APID=101, CCSDS_Version=0, CCSDS_Sec_Hdr_Flag=NotPresent, Port=2000, Value=3000, CCSDS_Type=TC, CCSDS_Group_Flags=Standalone, Packet_ID=4}:


        byte[] binary = null;
//        System.out.println("inside buildCommand MetaCommandProcessor binary:1" + binary);
        if (cmdContainer != null) {
            try {
                pcontext.mccProcessor.encode(mc);
            } catch (CommandEncodingException e) {
                throw new ErrorInCommand("Error when encoding command: " + e.getMessage());
            }
            int length = pcontext.getSize();//24
            binary = new byte[length]; // [B@15af699e
            System.arraycopy(bitbuf.array(), 0, binary, 0, length);  // [B@15af699e
        }
//        System.out.println("inside buildCommand MetaCommandProcessor end1:" + binary +":" + pcontext.getArgValues()); // [B@b47cbdc:{ArgName: CCSDS_Version argType:IntegerArgumentType name:CCSDS_Version_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 3, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Version rawValue: {0} engValue: {0}, ArgName: CCSDS_Type argType:BooleanArgumentType name:CCSDS_Type_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Type rawValue: {1} engValue: {true}, ArgName: CCSDS_Sec_Hdr_Flag argType:BooleanArgumentType name:CCSDS_Sec_Hdr_Flag_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Sec_Hdr_Flag rawValue: {0} engValue: {false}, ArgName: CCSDS_APID argType:IntegerArgumentType name:CCSDS_APID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 11, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_APID rawValue: {101} engValue: {101}, ArgName: CCSDS_Group_Flags argType:EnumeratedArgumentType: {0=(0=Continuation), 1=(1=First), 2=(2=Last), 3=(3=Standalone)} encoding:IntegerDataEncoding[sizeInBits: 2, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Group_Flags rawValue: {3} engValue: {Standalone}, ArgName: Packet_ID argType:IntegerArgumentType name:Packet_ID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 16, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: Packet_ID rawValue: {4} engValue: {4}, ArgName: Port argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)=name: Port rawValue: {2000} engValue: {2000}, ArgName: Value argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)=name: Value rawValue: {3000} engValue: {3000}}
//        System.out.println("inside buildCommand MetaCommandProcessor end2");
        return new CommandBuildResult(binary, pcontext.getArgValues()); // check just above values to check arguments
    }

    /**
     * Builds the argument values based on the user-provided arguments, initial values, and container-inherited
     * assignments.
     * 
     * The args are emptied as values are being used. If at the end of the call there are unused assignment, then one or
     * more invalid arguments were provided.
     * 
     * This function is called recursively.
     */
    private static void collectAndCheckArguments(TcProcessingContext pcontext, Map<String, Object> args)
            throws ErrorInCommand { // {CCSDS_APID=101, CCSDS_Version=0, CCSDS_Sec_Hdr_Flag=NotPresent, Port=2000, Value=3000, CCSDS_Type=TC, CCSDS_Group_Flags=Standalone, Packet_ID=4}:
//        System.out.println("inside collectAndCheckArguments in MetaCommandProcessor start1:" + pcontext);// EvaluatorInput [params=null, cmdArgs=[], cmdParams=null, cmdParamsCache=org.yamcs.parameter.LastValueCache@4b667d22]
        List<Argument> argList = pcontext.getCommand().getEffectiveArgumentList(); // it gives all ArgName: Port argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0) (it is one example it gives for CCSDS and Value also) defined in XTCE.xml
        //        System.out.println("inside collectAndCheckArguments in MetaCommandProcessor:" + argList); //[ArgName: CCSDS_Version argType:IntegerArgumentType name:CCSDS_Version_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 3, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgName: CCSDS_Type argType:BooleanArgumentType name:CCSDS_Type_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgName: CCSDS_Sec_Hdr_Flag argType:BooleanArgumentType name:CCSDS_Sec_Hdr_Flag_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgName: CCSDS_APID argType:IntegerArgumentType name:CCSDS_APID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 11, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgName: CCSDS_Group_Flags argType:EnumeratedArgumentType: {0=(0=Continuation), 1=(1=First), 2=(2=Last), 3=(3=Standalone)} encoding:IntegerDataEncoding[sizeInBits: 2, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgName: Packet_ID argType:IntegerArgumentType name:Packet_ID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 16, byteOrder: BIG_ENDIAN, encoding:UNSIGNED], ArgName: Port argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0), ArgName: Value argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)]
        List<Argument> unassigned = new ArrayList<>();

        // it will run only else part and collect parameter 2000 3000 and defined xml command definition parameters

        if (argList != null) {
            // check for each argument that we either have an assignment or an value
            for (Argument a : argList) {
//                System.out.println("inside loop in collectAndCheckArguments in MetaCommandProcessor1:" + a+":" + a.getName()+":" + a.getInitialValue() + ":" + a.getArgumentType().getInitialValue()); // ArgName: CCSDS_Version argType:IntegerArgumentType name:CCSDS_Version_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 3, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]:CCSDS_Version:null:null
                if (pcontext.hasArgumentValue(a)) {
                    continue;
                }
                Value argValue = null;
                Object argObj = null;
                if (!args.containsKey(a.getName())) {
                    argObj = a.getInitialValue();

                    if (argObj == null) {
                        argObj = a.getArgumentType().getInitialValue();
                    }

                    if (argObj == null) {
                        unassigned.add(a);
                        continue;
                    }
                } else {
                    Object value = args.remove(a.getName());
//                    System.out.println("inside loop in else collectAndCheckArguments in MetaCommandProcessor 5:"+ value); // in every iteration log value => 0 TC NotPresent 101 Standalone 4 2000 3000

                    try {
                        argObj = a.getArgumentType().convertType(value);
//                        System.out.println("inside loop in else collectAndCheckArguments in MetaCommandProcessor 6:"+ argObj); // // in every iteration log argObj => 0 true false 101 Standalone 4 2000 3000

                    } catch (Exception e) {
                        throw new ErrorInCommand("Cannot assign value to " + a.getName() + ": " + e.getMessage());
                    }
                }
                try {
                    pcontext.argumentTypeProcessor.checkRange(a.getArgumentType(), argObj);
                    argValue = DataTypeProcessor.getValueForType(a.getArgumentType(), argObj);
//                    System.out.println("inside loop in else collectAndCheckArguments in MetaCommandProcessor 7:"+ argValue);  // in every iteration log argValue => 0 true false 101 Standalone 4 2000 3000

                } catch (Exception e) {
                    throw new ErrorInCommand("Cannot assign value to " + a.getName() + ": " + e.getMessage());
                }
                pcontext.addArgumentValue(a, argValue);
            }
//            System.out.println("inside collectAndCheckArguments in MetaCommandProcessor end1:" + unassigned); // null
        }
        if (!unassigned.isEmpty()) {
            // some arguments may have been assigned by the checkRange method
            // for example arguments used as dynamic array sizes
            unassigned.removeAll(pcontext.getCmdArgs().keySet());
            if (!unassigned.isEmpty()) {
                throw new ErrorInCommand("No value provided for arguments: "
                        + unassigned.stream().map(arg -> arg.getName()).collect(Collectors.joining(", ", "[", "]")));
            }
        }
    }

    // look at the command container if it inherits another container using a condition list and add those parameters
    // with the respective values
    private static void collectParameters(Container container, Map<Parameter, Value> params) throws ErrorInCommand {
        System.out.println("inside collectParameters" );
        Container parent = container.getBaseContainer();
        if (parent != null) {
            MatchCriteria cr = container.getRestrictionCriteria();//null
            if (cr instanceof ComparisonList) {
                ComparisonList cl = (ComparisonList) cr;
                for (Comparison c : cl.getComparisonList()) {
                    if (c.getComparisonOperator() == OperatorType.EQUALITY) {
                        Parameter param = ((ParameterInstanceRef) c.getRef()).getParameter();
                        if (param != null) {
                            try {
                                Value v = ParameterTypeUtils.parseString(param.getParameterType(), c.getStringValue());
                                params.put(param, v);
                            } catch (IllegalArgumentException e) {
                                throw new ErrorInCommand("Cannot parse '" + c.getStringValue()
                                        + "' as value for parameter " + param.getQualifiedName());
                            }
                        }
                    }
                }
            }
        }
//        System.out.println("inside collectParameters params value:" + params);// {} means null
    }

    static public class CommandBuildResult {
        byte[] cmdPacket;
        Map<Argument, ArgumentValue> args;

        public CommandBuildResult(byte[] b, Map<Argument, ArgumentValue> args) {
            this.cmdPacket = b;
            this.args = args;
        }

        public byte[] getCmdPacket() {
            return cmdPacket;
        }
        // [B@15af699e
        public Map<Argument, ArgumentValue> getArgs() {
            return args;
        }
        // [B@b47cbdc:{ArgName: CCSDS_Version argType:IntegerArgumentType name:CCSDS_Version_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 3, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Version rawValue: {0} engValue: {0}, ArgName: CCSDS_Type argType:BooleanArgumentType name:CCSDS_Type_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Type rawValue: {1} engValue: {true}, ArgName: CCSDS_Sec_Hdr_Flag argType:BooleanArgumentType name:CCSDS_Sec_Hdr_Flag_Type, encoding: IntegerDataEncoding[sizeInBits: 1, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Sec_Hdr_Flag rawValue: {0} engValue: {false}, ArgName: CCSDS_APID argType:IntegerArgumentType name:CCSDS_APID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 11, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_APID rawValue: {101} engValue: {101}, ArgName: CCSDS_Group_Flags argType:EnumeratedArgumentType: {0=(0=Continuation), 1=(1=First), 2=(2=Last), 3=(3=Standalone)} encoding:IntegerDataEncoding[sizeInBits: 2, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: CCSDS_Group_Flags rawValue: {3} engValue: {Standalone}, ArgName: Packet_ID argType:IntegerArgumentType name:Packet_ID_Type sizeInBits:32 signed: false, encoding: IntegerDataEncoding[sizeInBits: 16, byteOrder: BIG_ENDIAN, encoding:UNSIGNED]=name: Packet_ID rawValue: {4} engValue: {4}, ArgName: Port argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)=name: Port rawValue: {2000} engValue: {2000}, ArgName: Value argType:StringArgumentType name:String_Type encoding: StringDataEncoding size: TERMINATION_CHAR(terminationChar=0)=name: Value rawValue: {3000} engValue: {3000}}
    }
}
