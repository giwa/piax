/* Generated By:JJTree: Do not edit this line. DCL_StartDCL.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=DCL_,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.piax.common.dcl.parser;

public
@SuppressWarnings("all")
class DCL_StartDCL extends SimpleNode {
  public DCL_StartDCL(int id) {
    super(id);
  }

  public DCL_StartDCL(DCLParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(DCLParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=c2d34d4963415430db05ba4c4345d5be (do not edit this line) */
