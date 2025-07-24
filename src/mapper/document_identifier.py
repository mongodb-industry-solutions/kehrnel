# src/mapper/document_identifier.py
"""Document type identification based on structure patterns"""

from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import json
import csv
from lxml import etree
import yaml

class DocumentPattern:
    """Represents a pattern for identifying document types"""
    def __init__(self, 
                 name: str, 
                 handler: str,
                 required_elements: List[str],
                 optional_elements: List[str] = None,
                 namespaces: Dict[str, str] = None,
                 xpath_patterns: List[str] = None,
                 csv_headers: List[str] = None):
        self.name = name
        self.handler = handler
        self.required_elements = required_elements
        self.optional_elements = optional_elements or []
        self.namespaces = namespaces or {}
        self.xpath_patterns = xpath_patterns or []
        self.csv_headers = csv_headers or []

# Define known document patterns
DOCUMENT_PATTERNS = [
    # PMSI CDA Pattern
    DocumentPattern(
        name="pmsi_cda",
        handler="xml",
        required_elements=["evenementPMSI", "periodeExercice"],
        optional_elements=["rss", "rum", "venue", "diagnostics", "actes"],
        namespaces={"cda": "urn:hl7-org:v3"},
        xpath_patterns=[
            "//evenementPMSI",
            "//periodeExercice[@dateDebut]",
            "//rss/rum"
        ]
    ),
    
    # Fiche Tumour Pattern (CDA)
    DocumentPattern(
        name="fiche_tumour_cda",
        handler="xml",
        required_elements=["ficheTumeur"],
        optional_elements=["dateCreation", "dateDiagnostic", "codeLesionnel", "TNM", "cote", "categorie"],
        namespaces={"cda": "urn:hl7-org:v3"},
        xpath_patterns=[
            "//ficheTumeur[@id]",
            "//section[title='Fiche Tumeur']"
        ]
    ),
    
    # SIMBAD Medication Administration Pattern
    DocumentPattern(
        name="simbad_medication_admin",
        handler="xml",
        required_elements=["M_Compte_rendu_administration", "Cycle", "Composant_administr"],
        optional_elements=["Protocole", "Séquence", "El_ment_administration"],
        xpath_patterns=[
            "//Messages/M_Compte_rendu_administration",
            "//Compte_rendu_admin/Cycle",
            "//Composant_administr[Code_composant]"
        ]
    ),
    
    # HL7v2 ADT Pattern
    DocumentPattern(
        name="hl7v2_adt",
        handler="hl7v2",
        required_elements=["MSH", "PID", "ADT"],
        optional_elements=["EVN", "PV1", "NK1"],
        xpath_patterns=[]  # Not applicable for HL7v2
    ),
    
    # Generic CDA Pattern (should be last to avoid false matches)
    DocumentPattern(
        name="generic_cda",
        handler="xml",
        required_elements=["ClinicalDocument"],
        optional_elements=["recordTarget", "author", "custodian"],
        namespaces={"cda": "urn:hl7-org:v3"},
        xpath_patterns=["//cda:ClinicalDocument"]
    ),
    
    # Biology Results CSV Pattern
    DocumentPattern(
        name="biology_results_csv",
        handler="csv",
        required_elements=[],  # CSV patterns don't have required elements in the same way
        csv_headers=["patient_id", "date_prelevement", "analyse", "resultat", "unite", "valeur_ref"]
    ),
    
    # Generic Lab Results CSV Pattern
    DocumentPattern(
        name="lab_results_csv",
        handler="csv",
        required_elements=[],  # CSV patterns don't have required elements in the same way
        csv_headers=["patient_id", "test_date", "test_name", "result", "unit", "reference_range"]
    )
]

class DocumentIdentifier:
    """Identifies document types based on structure and content patterns"""
    
    def __init__(self, patterns: List[DocumentPattern] = None):
        self.patterns = patterns or DOCUMENT_PATTERNS
        
    def identify_document(self, file_path: Path) -> Dict[str, Any]:
        """
        Identify document type and extract metadata
        
        Returns:
            Dict containing:
            - documentType: identified type or 'unknown'
            - handler: appropriate handler type
            - confidence: confidence score (0-1)
            - sampleData: sample extracted data
            - structure: document structure info
        """
        file_extension = file_path.suffix.lower()
        
        # Check content for HL7v2 first (can have various extensions)
        if self._is_hl7v2_file(file_path):
            return self._identify_hl7v2_document(file_path)
        
        # Determine handler based on file extension
        if file_extension in ['.xml', '.cda']:
            return self._identify_xml_document(file_path)
        elif file_extension in ['.csv', '.txt', '.tsv']:
            return self._identify_csv_document(file_path)
        elif file_extension == '.json':
            return self._identify_json_document(file_path)
        elif file_extension == '.hl7':
            return self._identify_hl7v2_document(file_path)
        else:
            return {
                "documentType": "unknown",
                "handler": "unknown",
                "confidence": 0.0,
                "sampleData": {},
                "structure": {"error": f"Unsupported file type: {file_extension}"}
            }
    
    def _identify_xml_document(self, file_path: Path) -> Dict[str, Any]:
        """Identify XML/CDA document type"""
        try:
            parser = etree.XMLParser(remove_blank_text=True, recover=True)
            tree = etree.parse(str(file_path), parser)
            root = tree.getroot()
            
            best_match = None
            best_score = 0.0
            scores = []  # For debugging
            
            # First, try specific patterns (non-generic)
            for pattern in self.patterns:
                if pattern.handler != "xml" or pattern.name == "generic_cda":
                    continue
                    
                score = self._calculate_xml_match_score(root, pattern)
                scores.append((pattern.name, score))
                
                if score > best_score and score > 0.6:  # Require at least 60% match
                    best_score = score
                    best_match = pattern
            
            # If no specific match found, try generic pattern
            if not best_match:
                for pattern in self.patterns:
                    if pattern.handler == "xml" and pattern.name == "generic_cda":
                        score = self._calculate_xml_match_score(root, pattern)
                        if score > 0.5:  # Lower threshold for generic
                            best_score = score
                            best_match = pattern
                        break
            
            if best_match:
                sample_data = self._extract_xml_sample_data(root, best_match)
                structure = self._extract_xml_structure(root)
                
                # Add debug info
                structure['pattern_scores'] = scores
                
                return {
                    "documentType": best_match.name,
                    "handler": "xml",
                    "confidence": best_score,
                    "sampleData": sample_data,
                    "structure": structure
                }
            
            # Default to unknown XML
            return {
                "documentType": "unknown_xml",
                "handler": "xml",
                "confidence": 0.3,
                "sampleData": {},
                "structure": self._extract_xml_structure(root)
            }
            
        except Exception as e:
            return {
                "documentType": "error",
                "handler": "xml",
                "confidence": 0.0,
                "sampleData": {},
                "structure": {"error": str(e)}
            }
    
    def _calculate_xml_match_score(self, root: etree._Element, pattern: DocumentPattern) -> float:
        """Calculate match score for XML document against pattern"""
        score = 0.0
        total_checks = 0
        
        # Check required elements - higher weight
        for element in pattern.required_elements:
            total_checks += 2.0  # Double weight for required elements
            elements = root.xpath(f"//*[local-name()='{element}']", namespaces=pattern.namespaces)
            if elements:
                score += 2.0
        
        # Check XPath patterns - highest weight for specific patterns
        for xpath in pattern.xpath_patterns:
            total_checks += 3.0  # Triple weight for specific XPath patterns
            try:
                if root.xpath(xpath, namespaces=pattern.namespaces):
                    score += 3.0
            except:
                pass
        
        # Check optional elements - lower weight
        for element in pattern.optional_elements:
            total_checks += 0.5
            if root.xpath(f"//*[local-name()='{element}']", namespaces=pattern.namespaces):
                score += 0.5
        
        # Penalty for generic patterns if specific elements are found
        if pattern.name == "generic_cda":
            # Check for specific document types that should NOT match generic
            specific_markers = [
                "//ficheTumeur",
                "//evenementPMSI",
                "//M_Compte_rendu_administration"
            ]
            for marker in specific_markers:
                if root.xpath(marker):
                    score *= 0.5  # Reduce score by half if specific markers found
                    break
        
        return score / total_checks if total_checks > 0 else 0.0
    
    def _is_hl7v2_file(self, file_path: Path) -> bool:
        """Check if file contains HL7v2 message"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                first_line = f.readline()
                return first_line.startswith('MSH|')
        except:
            return False
    
    def _identify_hl7v2_document(self, file_path: Path) -> Dict[str, Any]:
        """Identify HL7v2 message type"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            lines = content.strip().split('\n')
            if not lines or not lines[0].startswith('MSH|'):
                return {
                    "documentType": "invalid_hl7v2",
                    "handler": "hl7v2",
                    "confidence": 0.0,
                    "sampleData": {},
                    "structure": {"error": "Invalid HL7v2 format"}
                }
            
            # Parse MSH segment
            msh_fields = lines[0].split('|')
            message_type = msh_fields[8] if len(msh_fields) > 8 else ""
            
            # Extract sample data
            sample_data = {
                "messageType": message_type,
                "sendingApplication": msh_fields[2] if len(msh_fields) > 2 else "",
                "sendingFacility": msh_fields[3] if len(msh_fields) > 3 else "",
                "messageDateTime": msh_fields[6] if len(msh_fields) > 6 else ""
            }
            
            # Extract segments
            segments = []
            for line in lines:
                if line and len(line) >= 3:
                    segments.append(line[:3])
            
            # Determine specific type
            document_type = "hl7v2_message"
            if message_type.startswith("ADT"):
                document_type = "hl7v2_adt"
            elif message_type.startswith("ORU"):
                document_type = "hl7v2_lab_result"
            elif message_type.startswith("ORM"):
                document_type = "hl7v2_order"
            
            return {
                "documentType": document_type,
                "handler": "hl7v2",
                "confidence": 1.0,
                "sampleData": sample_data,
                "structure": {
                    "segments": list(set(segments)),
                    "messageType": message_type,
                    "segmentCount": len(lines)
                }
            }
            
        except Exception as e:
            return {
                "documentType": "error",
                "handler": "hl7v2",
                "confidence": 0.0,
                "sampleData": {},
                "structure": {"error": str(e)}
            }
    
    def _extract_xml_sample_data(self, root: etree._Element, pattern: DocumentPattern) -> Dict[str, Any]:
        """Extract sample data from XML based on pattern"""
        sample_data = {}
        
        # Extract values from required elements
        for element in pattern.required_elements[:5]:  # Limit to first 5
            nodes = root.xpath(f"//*[local-name()='{element}']", namespaces=pattern.namespaces)
            if nodes and nodes[0].text:
                sample_data[element] = nodes[0].text.strip()
        
        return sample_data
    
    def _extract_xml_structure(self, root: etree._Element) -> Dict[str, Any]:
        """Extract structural information from XML"""
        # Get unique element names
        elements = set()
        for elem in root.iter():
            local_name = etree.QName(elem).localname
            if local_name:
                elements.add(local_name)
        
        # Get namespaces
        namespaces = {}
        for prefix, uri in root.nsmap.items():
            if prefix:
                namespaces[prefix] = uri
        
        return {
            "rootElement": etree.QName(root).localname,
            "namespaces": namespaces,
            "elements": sorted(list(elements))[:20],  # First 20 elements
            "elementCount": len(elements)
        }
    
    def _identify_csv_document(self, file_path: Path) -> Dict[str, Any]:
        """Identify CSV document type"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Read first few lines to detect delimiter
                sample = f.read(1024)
                f.seek(0)
                
                # Detect delimiter
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                # Read headers
                reader = csv.DictReader(f, delimiter=delimiter)
                headers = reader.fieldnames
                
                # Read first row for sample
                first_row = next(reader, None)
            
            best_match = None
            best_score = 0.0
            
            for pattern in self.patterns:
                if pattern.handler != "csv":
                    continue
                    
                score = self._calculate_csv_match_score(headers, pattern)
                if score > best_score:
                    best_score = score
                    best_match = pattern
            
            structure = {
                "headers": headers,
                "delimiter": delimiter,
                "sampleRow": first_row
            }
            
            if best_match and best_score > 0.5:
                return {
                    "documentType": best_match.name,
                    "handler": "csv",
                    "confidence": best_score,
                    "sampleData": first_row or {},
                    "structure": structure
                }
            
            return {
                "documentType": "unknown_csv",
                "handler": "csv",
                "confidence": 0.3,
                "sampleData": first_row or {},
                "structure": structure
            }
            
        except Exception as e:
            return {
                "documentType": "error",
                "handler": "csv",
                "confidence": 0.0,
                "sampleData": {},
                "structure": {"error": str(e)}
            }
    
    def _calculate_csv_match_score(self, headers: List[str], pattern: DocumentPattern) -> float:
        """Calculate match score for CSV headers against pattern"""
        if not pattern.csv_headers or not headers:
            return 0.0
        
        # Normalize headers for comparison
        norm_headers = [h.lower().replace(' ', '_') for h in headers]
        norm_pattern = [h.lower().replace(' ', '_') for h in pattern.csv_headers]
        
        matches = sum(1 for h in norm_pattern if h in norm_headers)
        return matches / len(norm_pattern)
    
    def _identify_json_document(self, file_path: Path) -> Dict[str, Any]:
        """Identify JSON document type"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if it's an openEHR composition
            if isinstance(data, dict):
                if data.get("_type") == "COMPOSITION":
                    return {
                        "documentType": "openehr_composition",
                        "handler": "json",
                        "confidence": 1.0,
                        "sampleData": {
                            "name": data.get("name", {}).get("value", "Unknown"),
                            "archetype_node_id": data.get("archetype_node_id", "")
                        },
                        "structure": {
                            "type": "openEHR Composition",
                            "topLevelKeys": list(data.keys())[:10]
                        }
                    }
                elif data.get("tree") and data.get("templateId"):
                    return {
                        "documentType": "openehr_web_template",
                        "handler": "json",
                        "confidence": 1.0,
                        "sampleData": {
                            "templateId": data.get("templateId", "Unknown")
                        },
                        "structure": {
                            "type": "openEHR Web Template",
                            "topLevelKeys": list(data.keys())
                        }
                    }
            
            # Generic JSON
            return {
                "documentType": "unknown_json",
                "handler": "json",
                "confidence": 0.3,
                "sampleData": {},
                "structure": {
                    "type": type(data).__name__,
                    "keys": list(data.keys())[:10] if isinstance(data, dict) else []
                }
            }
            
        except Exception as e:
            return {
                "documentType": "error",
                "handler": "json",
                "confidence": 0.0,
                "sampleData": {},
                "structure": {"error": str(e)}
            }
    
    def add_pattern(self, pattern: DocumentPattern):
        """Add a new document pattern"""
        self.patterns.append(pattern)
    
    def list_patterns(self) -> List[str]:
        """List all registered document patterns"""
        return [p.name for p in self.patterns]