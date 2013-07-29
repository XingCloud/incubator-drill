package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.RecordBatch;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/2/13
 * Time: 7:00 PM
 */
public class FunctionEvaluatorRegistry {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionEvaluatorRegistry.class);

    static Map<String, Constructor<? extends BasicEvaluator>> map;

    static {
        final String scanPackage = "org.apache.drill.exec";

        String s = FilterBuilder.Include.prefix(scanPackage);

        Reflections r = new Reflections(new ConfigurationBuilder().filterInputsBy(new FilterBuilder().include(s))
                .setUrls(ClasspathHelper.forPackage(scanPackage))
                .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner(), new ResourcesScanner()));

        Set<Class<? extends BasicEvaluator>> providerClasses = r.getSubTypesOf(BasicEvaluator.class);
        Map<String, Constructor<? extends BasicEvaluator>> funcs = new HashMap<String, Constructor<? extends BasicEvaluator>>();
        for (Class<? extends BasicEvaluator> c : providerClasses) {
            try {
                FunctionEvaluator annotation = c.getAnnotation(FunctionEvaluator.class);
                if (annotation == null) {
                    // only basic evaluator marked with a function evaluator interface will be examinged.
                    continue;
                }

                Constructor<? extends BasicEvaluator> constructor = c.getConstructor(RecordBatch.class, FunctionArguments.class);


                funcs.put(annotation.value(), constructor);
            } catch (NoSuchMethodException e) {
                logger.warn(
                        "Unable to register Basic Evaluator {} because it does not have a constructor that accepts arguments of [Simplevh, ArgumentEvaluators] as its arguments.",
                        c);
            } catch (SecurityException e) {
                logger.warn("Unable to register Basic Evaluator {} because of security exception.", c, e);
            }
        }

        map = Collections.unmodifiableMap(funcs);
    }

    public static BasicEvaluator getEvaluator(String name, FunctionArguments args, RecordBatch recordBatch) {
        // TODO exception handle
        Constructor<? extends BasicEvaluator> c = map.get(name);
        if (c == null) throw new RuntimeException(String.format("Unable to find requested basic evaluator %s.", name));
        try {
            try {
                BasicEvaluator e = c.newInstance(recordBatch, args);
                return e;
            } catch (InvocationTargetException e) {
                throw new RuntimeException("");
            }
        } catch (RuntimeException | IllegalAccessException | InstantiationException ex) {
            throw new RuntimeException(String.format("Failure while attempting to create a new evaluator of type '%s'.", name),
                    ex);
        }

    }


}
