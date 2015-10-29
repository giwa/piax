/*
 * DependencyVisitor.java - ASM visitors
 * 
 * Copyright (c) 2009-2015 PIAX development team
 * Copyright (c) 2006-2008 Osaka University
 * Copyright (c) 2004-2005 BBR Inc, Osaka University
 * 
 * Permission is hereby granted, free of charge, to any person obtaining 
 * a copy of this software and associated documentation files (the 
 * "Software"), to deal in the Software without restriction, including 
 * without limitation the rights to use, copy, modify, merge, publish, 
 * distribute, sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do so, subject to 
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY 
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * Revision History:
 * ---
 * 2007/01/12 implemented by M. Yoshida.
 *       this code is modified from the ASM3.0 samples. 
 * $Id: DependencyVisitor.java 718 2013-07-07 23:49:08Z yos $
 */
package org.piax.agent.impl.asm;

import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

/**
 * Implementation class of ASM visitors.
 */
public class DependencyVisitor 
        implements AnnotationVisitor, SignatureVisitor,
        ClassVisitor, FieldVisitor, MethodVisitor {

    private Set<String> referedClasses;
    private String myClassName; // binary name

    public DependencyVisitor() {
        referedClasses = new HashSet<String>();
        myClassName = "";
    }

    public Set<String> getReferedClasses() {
        return referedClasses;
    }

    public void clearClasses() {
        referedClasses.clear();
    }

    // ClassVisitor

    public void visit(final int version, final int access, final String name,
            final String signature, final String superName,
            final String[] interfaces) {
        myClassName = name.replace('/', '.');

        if (signature == null) {
            addName(superName);
            addNames(interfaces);
        } else {
            addSignature(signature);
        }
    }

    public AnnotationVisitor visitAnnotation(final String desc,
            final boolean visible) {
        addDesc(desc);
        return this;
    }

    public void visitAttribute(final Attribute attr) {
    }

    public FieldVisitor visitField(final int access, final String name,
            final String desc, final String signature, final Object value) {
        if (signature == null) {
            addDesc(desc);
        } else {
            addTypeSignature(signature);
        }
        if (value instanceof Type) {
            addType((Type) value);
        }
        return this;
    }

    public MethodVisitor visitMethod(final int access, final String name,
            final String desc, final String signature, final String[] exceptions) {
        if (signature == null) {
            addMethodDesc(desc);
        } else {
            addSignature(signature);
        }
        addNames(exceptions);
        return this;
    }

    public void visitSource(final String source, final String debug) {
    }

    public void visitInnerClass(final String name, final String outerName,
            final String innerName, final int access) {
        /*
         * outerName か innerName に null が入ってくる場合を考慮
         * 無名内部クラスを使ったケースへの対応
         * 
         * suggestion from Dr. K. Abe (2008/10/24)
         */
        if (outerName != null && innerName != null) {
            addName(outerName + "$" + innerName);
        }
    }

    public void visitOuterClass(final String owner, final String name,
            final String desc) {
        //addName(owner);
        //addMethodDesc(desc);
    }

    // MethodVisitor

    public AnnotationVisitor visitParameterAnnotation(final int parameter,
            final String desc, final boolean visible) {
        addDesc(desc);
        return this;
    }

    public void visitTypeInsn(final int opcode, final String desc) {
        if (desc.charAt(0) == '[') {
            addDesc(desc);
        } else {
            addName(desc);
        }
    }

    public void visitFieldInsn(final int opcode, final String owner,
            final String name, final String desc) {
        addName(owner);
        addDesc(desc);
    }

    public void visitMethodInsn(final int opcode, final String owner,
            final String name, final String desc) {
        if (owner.charAt(0) == '[') {
            addDesc(owner);
        } else {
            addName(owner);
        }
        addMethodDesc(desc);
    }

    public void visitLdcInsn(final Object cst) {
        if (cst instanceof Type) {
            addType((Type) cst);
        }
    }

    public void visitMultiANewArrayInsn(final String desc, final int dims) {
        addDesc(desc);
    }

    public void visitLocalVariable(final String name, final String desc,
            final String signature, final Label start, final Label end,
            final int index) {
        addTypeSignature(signature);
    }

    public AnnotationVisitor visitAnnotationDefault() {
        return this;
    }

    public void visitCode() {
    }

    public void visitFrame(final int type, final int nLocal,
            final Object[] local, final int nStack, final Object[] stack) {
    }

    public void visitInsn(final int opcode) {
    }

    public void visitIntInsn(final int opcode, final int operand) {
    }

    public void visitVarInsn(final int opcode, final int var) {
    }

    public void visitJumpInsn(final int opcode, final Label label) {
    }

    public void visitLabel(final Label label) {
    }

    public void visitIincInsn(final int var, final int increment) {
    }

    public void visitTableSwitchInsn(final int min, final int max,
            final Label dflt, final Label[] labels) {
    }

    public void visitLookupSwitchInsn(final Label dflt, final int[] keys,
            final Label[] labels) {
    }

    public void visitTryCatchBlock(final Label start, final Label end,
            final Label handler, final String type) {
        addName(type);
    }

    public void visitLineNumber(final int line, final Label start) {
    }

    public void visitMaxs(final int maxStack, final int maxLocals) {
    }

    // AnnotationVisitor

    public void visit(final String name, final Object value) {
        if (value instanceof Type) {
            addType((Type) value);
        }
    }

    public void visitEnum(final String name, final String desc,
            final String value) {
        addDesc(desc);
    }

    public AnnotationVisitor visitAnnotation(final String name,
            final String desc) {
        addDesc(desc);
        return this;
    }

    public AnnotationVisitor visitArray(final String name) {
        return this;
    }

    // SignatureVisitor

    public void visitFormalTypeParameter(final String name) {
    }

    public SignatureVisitor visitClassBound() {
        return this;
    }

    public SignatureVisitor visitInterfaceBound() {
        return this;
    }

    public SignatureVisitor visitSuperclass() {
        return this;
    }

    public SignatureVisitor visitInterface() {
        return this;
    }

    public SignatureVisitor visitParameterType() {
        return this;
    }

    public SignatureVisitor visitReturnType() {
        return this;
    }

    public SignatureVisitor visitExceptionType() {
        return this;
    }

    public void visitBaseType(final char descriptor) {
    }

    public void visitTypeVariable(final String name) {
        // TODO verify
    }

    public SignatureVisitor visitArrayType() {
        return this;
    }

    public void visitClassType(final String name) {
        addName(name);
    }

    public void visitInnerClassType(final String name) {
        addName(name);
    }

    public void visitTypeArgument() {
    }

    public SignatureVisitor visitTypeArgument(final char wildcard) {
        return this;
    }

    // common

    public void visitEnd() {
    }

    // ---------------------------------------------

    private void addName(final String name) {
        if (name == null) {
            return;
        }
        String bname = name.replace('/', '.');
        if (bname.equals(myClassName)) {
            return;
        }
        referedClasses.add(bname);
    }

    private void addNames(final String[] names) {
        if (names == null) {
            return;
        }
        for (String name : names) {
            addName(name);
        }
    }

    private void addDesc(final String desc) {
        addType(Type.getType(desc));
    }

    private void addMethodDesc(final String desc) {
        addType(Type.getReturnType(desc));
        Type[] types = Type.getArgumentTypes(desc);
        for (int i = 0; i < types.length; i++) {
            addType(types[i]);
        }
    }

    private void addType(final Type t) {
        switch (t.getSort()) {
        case Type.ARRAY:
            addType(t.getElementType());
            break;
        case Type.OBJECT:
            addName(t.getClassName().replace('.', '/'));
            break;
        }
    }

    private void addSignature(final String signature) {
        if (signature != null) {
            new SignatureReader(signature).accept(this);
        }
    }

    private void addTypeSignature(final String signature) {
        if (signature != null) {
            new SignatureReader(signature).acceptType(this);
        }
    }
}
