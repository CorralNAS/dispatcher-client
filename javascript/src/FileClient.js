/*
 * Copyright 2015 iXsystems, Inc.
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */
import { getErrno, getCode } from "./ErrnoCodes.js"

export class FileClient
{
    constructor( client ) {
      this.client = client;
      this.socket = null;
      this.token = null;
      this.authenticated = false;

      /* Callbacks */
      this.onOpen = () => { };
      this.onClose = () => { };
      this.onData = () => { };
      this.onError = () => { };
    }

    __onopen() {
      this.socket.send( JSON.stringify(
        { token: this.token }
      ) );
      this.onOpen();
    }


    __onclose() {
      this.onClose();
    }

    __onerror( ev ) {
      alert( getErrno( ev.code ) );
      this.onError();
    }

    __onmessage( msg ) {
      if ( !this.authenticated ) {
        let payload = JSON.parse( msg.data );
        if ( payload.status == "ok" ) {
          this.authenticated = true;
        } else {
          /* XXX error */
          console.log( "FileConnection not authenticated? paylod: ", paylod );
        }

        return;
      }

      var reader = new FileReader();
      reader.onload = () => { this.onData( reader.result ); };
      reader.readAsBinaryString( msg.data );
    }

    upload( destPath, size, mode ) {
      /* Request upload file connection websocket */
      this.client.call
        ( "filesystem.upload"
        , [ destPath, size, mode ]
        , result => {
            this.token = result;
            this.socket = new WebSocket( `ws://${this.client.hostname}:5000/file` );
            if ( this.socket instanceof WebSocket ) {
              Object.assign( this.socket
                           , { onopen: this.__onopen.bind( this )
                             , onmessage: this.__onmessage.bind( this )
                             , onerror: this.__onerror.bind( this )
                             , onclose: this.__onclose.bind( this )
                             }
                           , this
              );
            } else {
              throw new Error( "Was unable to create a WebSocket instance for FileConnection" );
            }
        }
      );
    }

    download( path ) {
      /* Request download file connection websocket */
      this.client.call
        ( "filesystem.download"
        , [ path ]
        , result => {
          this.token = result;
          this.socket = new WebSocket( `ws://${this.client.hostname}:5000/file` );
          if ( this.socket instanceof WebSocket ) {
            Object.assign( this.socket
                         , { onopen: this.__onopen.bind( this )
                           , onmessage: this.__onmessage.bind( this )
                           , onerror: this.__onerror.bind( this )
                           , onclose: this.__onclose.bind( this )
                           }
                         , this
            );
          } else {
            throw new Error( "Was unable to create a WebSocket instance for FileConnection" );
          }
        }
      );
    }

    disconnect() {
      if ( this.socket === null ) { };
      this.socket.close();
    }

    send( data ) {
      this.socket.send( data );
    }
}
