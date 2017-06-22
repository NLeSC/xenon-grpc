package nl.esciencecenter.xenon.grpc.jobs;

import java.io.IOException;
import java.io.InputStream;

import nl.esciencecenter.xenon.grpc.XenonProto;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

class JobOutputStreamsForwarder {
    
	private final XenonProto.JobOutputStreams.Builder builder;
    private final StreamObserver<XenonProto.JobOutputStreams> observer;

	private static final int BUFFER_SIZE = 1024;
    
    private int streamsDone = 0;
    
    class StreamForwarder extends Thread {
    	
    	private final byte[] buffer = new byte[BUFFER_SIZE];
    
        private final InputStream in;
        
        private boolean stdout = false;
        
        StreamForwarder(InputStream in, boolean stdout) {
        	super("Stream forwarder " + (stdout ? "stdout" : "stderr"));
        	this.in = in;
            this.stdout = stdout;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    int read = in.read(buffer);

                    if (read > 0) { 
                    	XenonProto.JobOutputStreams response;
                    	
                    	if (stdout) {
                    		response = builder.clearStderr().setStdout(ByteString.copyFrom(buffer, 0, read)).build();
                    	} else { 
                    		response = builder.clearStdout().setStderr(ByteString.copyFrom(buffer, 0, read)).build();
                    	}
                    	
                    	writeOut(response);

                    } else if (read == -1) {
                    	close();
                        return;
                    }                }
            } catch (IOException e) {
            	observer.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
            }
        }
    }
    
    JobOutputStreamsForwarder(StreamObserver<XenonProto.JobOutputStreams> responseWriter, InputStream stderr, InputStream stdout) {
        this.observer = responseWriter;
        builder = XenonProto.JobOutputStreams.newBuilder();
        // We should fully read the in and output streams here (non blocking and in parallel) and forward the data to the responseWriter.
        new StreamForwarder(stdout, true).start();
        new StreamForwarder(stderr, false).start();
    }

    private synchronized void writeOut(XenonProto.JobOutputStreams response) {
    	observer.onNext(response);
    }

    public synchronized void close() {
    	if (++streamsDone == 2) { 
    		observer.onCompleted();
    	}
    }
}
