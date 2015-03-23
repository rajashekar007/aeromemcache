/*
 * xmlUtils.h
 *
 *  Created on: Mar 17, 2012
 *      Author: carolyn
 */

#ifndef XMLUTILS_H_
#define XMLUTILS_H_

#include <libxml2/libxml/xmlexports.h>
#include <libxml2/libxml/xmlstring.h>
#include <libxml2/libxml/tree.h>

xmlNode *xmlNodeGetFirstChild(xmlNode *node, xmlChar *childName);
xmlNode *xmlNodeGetNextChild(xmlNode *curChild);


#endif /* XMLUTILS_H_ */
