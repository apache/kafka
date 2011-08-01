/*
 *  Copyright (c) 2011 NeuStar, Inc.
 *  All rights reserved.  
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at 
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  NeuStar, the Neustar logo and related names and logos are registered
 *  trademarks, service marks or tradenames of NeuStar, Inc. All other 
 *  product names, company names, marks, logos and symbols may be trademarks
 *  of their respective owners.
 */

package kafka


import (
  "encoding/binary"
)


func uint16bytes(value int) []byte {
  result := make([]byte, 2)
  binary.BigEndian.PutUint16(result, uint16(value))
  return result
}

func uint32bytes(value int) []byte {
  result := make([]byte, 4)
  binary.BigEndian.PutUint32(result, uint32(value))
  return result
}

func uint32toUint32bytes(value uint32) []byte {
  result := make([]byte, 4)
  binary.BigEndian.PutUint32(result, value)
  return result
}

func uint64ToUint64bytes(value uint64) []byte {
  result := make([]byte, 8)
  binary.BigEndian.PutUint64(result, value)
  return result
}
