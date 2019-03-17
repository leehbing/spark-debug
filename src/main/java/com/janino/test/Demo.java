package com.janino.test;

import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;

/**
 * @Author: hongbing.li
 * @Date: 4/3/2019 3:31 PM
 */
public class Demo {
    public static void main(String[] args) throws Exception{
        testExpressionEvaluator();
        testScriptEvaluator();
    }

    private static void testExpressionEvaluator() throws Exception{
        IExpressionEvaluator ee = new ExpressionEvaluator();
        ee.setExpressionType(int.class);
        ee.setParameters(
                new String[] {"c", "d"},            // parameterNames
                new Class[] {int.class, int.class}  // parameterTypes
        );

        // compile the expression once; relatively slow.
        ee.cook("c > d ? c : d");

        // Evaluate it with varying parameter values ; very fast.
        Integer res = (Integer) ee.evaluate(
                new Object[] { 10, 11}          // arguments
        );
        System.out.println("res = " + res);
    }

    private static void testScriptEvaluator() throws Exception{
        // create "ScriptEvaluator" object
        IScriptEvaluator se = new ScriptEvaluator();
        se.setReturnType(boolean.class);
        se.cook(
                "System.out.println(\" Hello world\");\n"
                +"return true; \n"

        );
        // Evaluate script with actual parameter values.
        Object res = se.evaluate(
                new Object[0]           //arguments
        );
        System.out.println("res = " + res);

    }


}
