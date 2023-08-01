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
    final ProcessorData pdata;

    public MetaCommandProcessor(ProcessorData pdata) {
        this.pdata = pdata;
    }

    public CommandBuildResult buildCommand(MetaCommand mc, Map<String, Object> argAssignmentList)
            throws ErrorInCommand {
        System.out.println("inside buildCommand MetaCommandProcessor1::" + mc.getCommandContainer().getName() +" :" + argAssignmentList);
        return buildCommand(pdata, mc, argAssignmentList);
    }

    public static CommandBuildResult buildCommand(ProcessorData pdata, MetaCommand mc,
            Map<String, Object> argAssignmentList) throws ErrorInCommand {
        System.out.println("inside buildCommand MetaCommandProcessor::1" +pdata+":" + mc +" :" + argAssignmentList);
        if (mc.isAbstract()) {
            throw new ErrorInCommand("Not building command " + mc.getQualifiedName() + " because it is abstract");
        }
        System.out.println("inside buildCommand MetaCommandProcessor::2" + mc.isAbstract());
        ProcessorConfig procConf = pdata.getProcessorConfig();
        System.out.println("inside buildCommand MetaCommandProcessor procConf::3" + procConf);

        Map<Parameter, Value> params = new HashMap<>();

        CommandContainer cmdContainer = mc.getCommandContainer();
        System.out.println("inside buildCommand MetaCommandProcessor cmdContainer::4" + cmdContainer.getQualifiedName());

        if (cmdContainer == null && !procConf.allowContainerlessCommands()) {
            throw new ErrorInCommand("MetaCommand " + mc.getName()
                    + " has no container (and the processor option allowContainerlessCommands is set to false)");
        }

        if (cmdContainer != null) {
            collectParameters(cmdContainer, params);
        }
        BitBuffer bitbuf = new BitBuffer(new byte[procConf.getMaxCommandSize()]);
        System.out.println("inside buildCommand MetaCommandProcessor bitbuf::5" + bitbuf);
        TcProcessingContext pcontext = new TcProcessingContext(mc, pdata, params, bitbuf, 0);
        System.out.println("inside buildCommand MetaCommandProcessor TcProcessingContext pcontext::6" + pcontext);

        Map<String, Object> argAssignment = new HashMap<>(argAssignmentList);
        System.out.println("inside buildCommand MetaCommandProcessor argAssignment::7" + argAssignment);


        List<ArgumentAssignment> inheritedAssignment = mc.getEffectiveArgumentAssignmentList();
        System.out.println("inside buildCommand MetaCommandProcessor inheritedAssignment List::8" + inheritedAssignment);

        for (ArgumentAssignment aa : inheritedAssignment) {
            System.out.println("inside buildCommand MetaCommandProcessor inheritedAssignment List::9" + aa );

            if (argAssignment.containsKey(aa.getArgumentName())) {
                throw new ErrorInCommand("Cannot overwrite the argument " + aa.getArgumentName()
                        + " which is defined in the inheritance assignment list");
            }
            argAssignment.put(aa.getArgumentName(), aa.getArgumentValue());
            System.out.println("inside buildCommand MetaCommandProcessor inheritedAssignment List::10" + aa.getArgumentName() +":" +  aa.getArgumentValue() );

        }
        System.out.println("inside buildCommand MetaCommandProcessor::11" + argAssignment);
        // it gets every argAssignment here which I have send from Yamcs
        collectAndCheckArguments(pcontext, argAssignment);

        byte[] binary = null;
        System.out.println("inside buildCommand MetaCommandProcessor::" + binary +" and cmdContainer:12" + cmdContainer);

        if (cmdContainer != null) {
            try {
                System.out.println("inside buildCommand MetaCommandProcessor for encoding::13");

                pcontext.mccProcessor.encode(mc);
            } catch (CommandEncodingException e) {
                throw new ErrorInCommand("Error when encoding command:14 " + e.getMessage());
            }

            int length = pcontext.getSize();
            binary = new byte[length];
            System.arraycopy(bitbuf.array(), 0, binary, 0, length);
        }
        return new CommandBuildResult(binary, pcontext.getArgValues());
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
            throws ErrorInCommand {
        System.out.println("inside collectAndCheckArguments MetaCommandProcessor::1" + pcontext +":" + args+"::");

        List<Argument> argList = pcontext.getCommand().getEffectiveArgumentList();
        List<Argument> unassigned = new ArrayList<>();
        System.out.println("inside collectAndCheckArguments MetaCommandProcessor argList by pcontext::2" + argList);

        if (argList != null) {
            // check for each argument that we either have an assignment or an value
            System.out.println("inside collectAndCheckArguments MetaCommandProcessor argList ::3" + "check for each argument that we either have an assignment or an value");

            for (Argument a : argList) {
                System.out.println("inside collectAndCheckArguments MetaCommandProcessor argList by Argument a::4" + a);
                System.out.println("inside collectAndCheckArguments MetaCommandProcessor argList by Argument a::5" + pcontext.hasArgumentValue(a));
                System.out.println("inside collectAndCheckArguments MetaCommandProcessor argList by Argument a.getName()::6" + a.getName() + "andInitialValue" + a.getInitialValue() +
                        "anda.getArgumentType().getInitialValue()" + a.getArgumentType().getInitialValue());

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
                    try {
                        argObj = a.getArgumentType().convertType(value);
                    } catch (Exception e) {
                        throw new ErrorInCommand("Cannot assign value to " + a.getName() + ": " + e.getMessage());
                    }
                }
                try {
                    pcontext.argumentTypeProcessor.checkRange(a.getArgumentType(), argObj);
                    argValue = DataTypeProcessor.getValueForType(a.getArgumentType(), argObj);
                } catch (Exception e) {
                    throw new ErrorInCommand("Cannot assign value to " + a.getName() + ": " + e.getMessage());
                }
                pcontext.addArgumentValue(a, argValue);
            }
        }
        System.out.println("inside collectAndCheckArguments MetaCommandProcessor pcontect::7" + pcontext);
        System.out.println("inside collectAndCheckArguments MetaCommandProcessor unassigned::8" + unassigned);
        System.out.println("inside collectAndCheckArguments MetaCommandProcessor pcontext.getCmdArgs().keySet()::9" + pcontext.getCmdArgs().keySet());

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
        System.out.println("inside collectParameters in MetaCommandProcessor::1" + container + " :"+ ":" + params);
        Container parent = container.getBaseContainer();
        System.out.println("inside collectParameters in MetaCommandProcessor parent::2" + parent);

        if (parent != null) {
            MatchCriteria cr = container.getRestrictionCriteria();
            System.out.println("inside collectParameters in MetaCommandProcessor criteria::3" + cr);

            if (cr instanceof ComparisonList) {
                ComparisonList cl = (ComparisonList) cr;
                for (Comparison c : cl.getComparisonList()) {
                    if (c.getComparisonOperator() == OperatorType.EQUALITY) {
                        Parameter param = ((ParameterInstanceRef) c.getRef()).getParameter();
                        System.out.println("inside collectParameters in MetaCommandProcessor param::4" + param +" :" + param.getParameterType() + ":" + c.getStringValue());
                        if (param != null) {
                            try {
                                Value v = ParameterTypeUtils.parseString(param.getParameterType(), c.getStringValue());
                                params.put(param, v);
                                System.out.println("inside collectParameters in MetaCommandProcessor param::5" + param +" :" + v + ":" );

                            } catch (IllegalArgumentException e) {
                                throw new ErrorInCommand("Cannot parse '" + c.getStringValue()
                                        + "' as value for parameter " + param.getQualifiedName());
                            }
                        }
                    }
                }
            }
        }
    }

    static public class CommandBuildResult {
        byte[] cmdPacket;
        Map<Argument, ArgumentValue> args;

        public CommandBuildResult(byte[] b, Map<Argument, ArgumentValue> args) {
            this.cmdPacket = b;
            this.args = args;
        }

        public byte[] getCmdPacket() {
            System.out.println("inside getCmdpacket 11111111111111:: "+cmdPacket);
            return cmdPacket;
        }

        public Map<Argument, ArgumentValue> getArgs() {
            return args;
        }
    }
}
